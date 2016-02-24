{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.GC
-- Copyright   : (c) 2015-16 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Asynchronous garbage collector for the database.
--
-- The GC creates fresh copies of both the log and the data (abstract) files,
-- fully /cleaned/, respectively /compacted/, and finally takes a master lock,
-- appends the transaction data that was added in the mean time, and uses the
-- 'swapDb' function to make the new files current. No expensive lock needs
-- to be taken in the beginning, since all indexes are purely functional and
-- we just take some pointers.
--
-- /Cleaning/ means removal of all versions of deleted records, and all but the
-- most recent version for the rest.
--
-- /Compacting/ means all records are reallocated contiguously starting from
-- 'DocAddress' 0. In particular, it creates a fresh empty 'gaps' index with the
-- help of 'Gaps.buildExtra'.
--
-- The complete operation takes __O(n*log(n))__ time, but the lock is held only
-- for __O(k*log(n))__ at the end, where k represents the number of new records,
-- which is similar to a normal query.
-- For this reason it is safe to run the GC at any time.
--
-- The database does not call 'Database.Muesli.Handle.performGC' by itself,
-- but leaves this to the user. Programs that only rarely
-- 'Database.Muesli.Query.delete' or 'Database.Muesli.Query.update' records
-- don't even need to run the GC, or they can make it an admin action.
----------------------------------------------------------------------------

module Database.Muesli.GC
  ( gcThread ) where

import           Control.Concurrent        (threadDelay)
import           Control.Monad             (forM_, unless, when)
import           Control.Monad.Trans       (MonadIO)
import           Data.Function             (on)
import           Data.IntMap.Strict        (IntMap)
import qualified Data.IntMap.Strict        as IntMap
import           Data.List                 (foldl', groupBy, sortOn)
import           Data.Map.Strict           ((\\))
import qualified Data.Map.Strict           as Map
import qualified Database.Muesli.Allocator as Gaps
import qualified Database.Muesli.IdSupply  as Ids
import           Database.Muesli.Indexes
import           Database.Muesli.State
import           Database.Muesli.Types

-- | Code for the GC thread forked by 'Database.Muesli.Handle.open'.
--
-- It listens for messages sent by 'Database.Muesli.Handle.performGC' through
-- 'gcState', and performs the GC operation.
gcThread :: forall l. LogState l => Handle l -> IO ()
gcThread h = do
  sgn <- withGC h $ \sgn -> do
    when (sgn == PerformGC) $ do
      (mainIdxOld, logCompOld) <- withMaster h $ \m ->
        return (m { keepTrans = True }, (mainIdx m, logComp m))
      let rs  = map head . filter (not . any recDeleted) $ IntMap.elems mainIdxOld
      let (rs2, dpos) = realloc 0 rs
      let rs' = sortOn recTransactionId $ map fst rs2
      let ts  = concatMap toTransRecord $ groupBy ((==) `on` recTransactionId) rs'
      let ids = foldl' (\s r -> Ids.reserve (recDocumentKey r) s) Ids.empty .
                map fromPending $ filter isPending ts
      let logPath = logDbPath (unHandle h)
      let logPathNew = logPath ++ ".new"
      withDb logPathNew $ \hnd -> do
        st <- logInit hnd
        logAppend (st :: l) ts
      let dataPath = dataDbPath (unHandle h)
      let dataPathNew = dataPath ++ ".new"
      buildDataFile dataPathNew rs2 h
      let mIdx = updateMainIndex   IntMap.empty rs'
      let uIdx = updateUniqueIndex IntMap.empty IntMap.empty rs'
      let iIdx = updateSortIndex   IntMap.empty IntMap.empty rs'
      let rIdx = updateFilterIndex IntMap.empty IntMap.empty rs'
      when (forceEval mIdx iIdx rIdx) $ withCommitSgn h $ \kill -> do
        withMaster h $ \nm -> do
          let (ncrs', dpos') = realloc dpos . concat . Map.elems $
                               logComp nm \\ logCompOld
          let (logp', dpos'') = realloc' dpos' $ logPend nm
          let ncrs = fst <$> ncrs'
          unless (null ncrs) . withDb logPathNew $ \hnd -> do
            st <- logInit hnd
            logAppend (st :: l) (toTransRecord ncrs)
            return ()
          st <- swapDb logPath logPathNew >>= logInit
          buildDataFile dataPathNew ncrs' h
          let gs = Gaps.buildExtra dpos'' . filter recDeleted $
                   ncrs ++ (map fst . concat $ Map.elems logp')
          let m = MasterState { logState  = st
                              , topTid    = topTid nm
                              , idSupply  = ids
                              , keepTrans = False
                              , gaps      = gs
                              , logPend   = logp'
                              , logComp   = Map.empty
                              , mainIdx   = updateMainIndex   mIdx ncrs
                              , unqIdx    = updateUniqueIndex mIdx uIdx ncrs
                              , sortIdx   = updateSortIndex   mIdx iIdx ncrs
                              , refIdx    = updateFilterIndex mIdx rIdx ncrs
                              }
          return (m, ())
        withData h $ \(DataState _ cache) -> do
          hnd' <- swapDb dataPath dataPathNew
          return (DataState hnd' cache, ())
        return (kill, ())
    let sgn' = if sgn == PerformGC then IdleGC else sgn
    return (sgn', sgn')
  unless (sgn == KillGC) $ do
    threadDelay $ 1000 * 1000
    gcThread h

isPending :: TransRecord -> Bool
isPending (Pending _)   = True
isPending (Completed _) = False

fromPending :: TransRecord -> LogRecord
fromPending (Pending r) = r

toTransRecord :: [LogRecord] -> [TransRecord]
toTransRecord rs = foldl' (\ts r -> Pending r : ts)
                   [Completed . recTransactionId $ head rs] rs

realloc :: DocAddress -> [LogRecord] -> ([(LogRecord, LogRecord)], DocAddress)
realloc st = foldl' f ([], st)
  where f (nrs, pos) r =
          if recDeleted r then ((r, r) : nrs, pos)
          else ((r { recAddress = pos }, r) : nrs, pos + recSize r)

realloc' :: DocAddress -> PendingIndex -> (PendingIndex, DocAddress)
realloc' st idx = (Map.fromList l, pos)
  where (l, pos) = foldl' f ([], st) $ Map.toList idx
        f (lst, p) (tid, rs) = ((tid, rs') : lst, p')
          where (rss', p') = realloc p $ fst <$> rs
                rs' = (fst <$> rss') `zip` (snd <$> rs)

forceEval :: IntMap a -> IntMap b -> IntMap c -> Bool
forceEval mIdx iIdx rIdx = IntMap.notMember (-1) mIdx &&
                           IntMap.size iIdx > (-1) &&
                           IntMap.size rIdx > (-1)

buildDataFile :: forall m l. (MonadIO m, LogState l) => FilePath ->
                 [(LogRecord, LogRecord)] -> Handle l -> m ()
buildDataFile path rs h =
  withDb path $ \hnd ->
    forM_ rs $ \(r, oldr) -> do
      bs <- withDataLock h $ \(DataState dh _) -> readDocument dh oldr
      writeDocument r bs (hnd :: DataHandleOf l)
