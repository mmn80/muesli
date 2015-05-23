{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.GC
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Garbage Collector for the database.
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
import           Foreign                   (sizeOf)

gcThread :: forall l d. (LogState l, DbHandle (LogHandle l), DataHandle d) =>
                         Handle l d -> IO ()
gcThread h = do
  sgn <- withGC h $ \sgn -> do
    when (sgn == PerformGC) $ do
      (mainIdxOld, logCompOld) <- withMaster h $ \m ->
        return (m { keepTrans = True }, (mainIdx m, logComp m))
      let rs  = map head . filter (not . any docDel) $ IntMap.elems mainIdxOld
      let (rs2, dpos) = realloc 0 rs
      let rs' = sortOn docTID $ map fst rs2
      let ts  = concatMap toTRecs $ groupBy ((==) `on` docTID) rs'
      let ids = foldl' (\s r -> Ids.reserve (docID r) s) Ids.empty .
                map fromPending $ filter isPending ts
      let logPath = logFilePath (unHandle h)
      let logPathNew = logPath ++ ".new"
      withDb logPathNew $ \hnd -> do
        st <- logInit hnd
        logAppend (st :: l) ts
      let dataPath = dataFilePath (unHandle h)
      let dataPathNew = dataPath ++ ".new"
      buildDataFile dataPathNew rs2 h
      let mIdx = updateMainIdx IntMap.empty rs'
      let uIdx = updateUnqIdx  IntMap.empty rs'
      let iIdx = updateIntIdx  IntMap.empty rs'
      let rIdx = updateRefIdx  IntMap.empty rs'
      when (forceEval mIdx iIdx rIdx) $ withUpdateMan h $ \kill -> do
        withMaster h $ \nm -> do
          let (ncrs', dpos') = realloc dpos . concat . Map.elems $
                               logComp nm \\ logCompOld
          let (logp', dpos'') = realloc' dpos' $ logPend nm
          let ncrs = fst <$> ncrs'
          unless (null ncrs) . withDb logPathNew $ \hnd -> do
            st <- logInit hnd
            logAppend (st :: l) (toTRecs ncrs)
            return ()
          st <- swapDb logPath logPathNew >>= logInit
          buildDataFile dataPathNew ncrs' h
          let gs = Gaps.buildExtra dpos'' . filter docDel $
                   ncrs ++ (map fst . concat $ Map.elems logp')
          let m = MasterState { logState = st
                              , topTID    = topTID nm
                              , idSupply  = ids
                              , keepTrans = False
                              , gaps      = gs
                              , logPend   = logp'
                              , logComp   = Map.empty
                              , mainIdx   = updateMainIdx mIdx ncrs
                              , unqIdx    = updateUnqIdx  uIdx ncrs
                              , intIdx    = updateIntIdx  iIdx ncrs
                              , refIdx    = updateRefIdx  rIdx ncrs
                              }
          return (m, ())
        withData h $ \(DataState hnd cache) -> do
          hnd' <- swapDb dataPath dataPathNew
          return (DataState hnd' cache, ())
        return (kill, ())
    let sgn' = if sgn == PerformGC then IdleGC else sgn
    return (sgn', sgn')
  unless (sgn == KillGC) $ do
    threadDelay $ 1000 * 1000
    gcThread h

toTRecs :: [DocRecord] -> [TRec]
toTRecs rs = foldl' (\ts r -> Pending r : ts)
             [Completed . docTID $ head rs] rs

realloc :: Addr -> [DocRecord] -> ([(DocRecord, DocRecord)], Addr)
realloc st = foldl' f ([], st)
  where f (nrs, pos) r =
          if docDel r then ((r, r) : nrs, pos)
          else ((r { docAddr = pos }, r) : nrs, pos + docSize r)

realloc' :: Addr -> LogPending -> (LogPending, Addr)
realloc' st idx = (Map.fromList l, pos)
  where (l, pos) = foldl' f ([], st) $ Map.toList idx
        f (lst, p) (tid, rs) = ((tid, rs') : lst, p')
          where (rss', p') = realloc p $ fst <$> rs
                rs' = (fst <$> rss') `zip` (snd <$> rs)

forceEval :: IntMap a -> IntMap b -> IntMap c -> Bool
forceEval mIdx iIdx rIdx = IntMap.notMember (-1) mIdx &&
                           IntMap.size iIdx > (-1) &&
                           IntMap.size rIdx > (-1)

buildDataFile :: forall m l d. (MonadIO m, DataHandle d) => FilePath ->
                 [(DocRecord, DocRecord)] -> Handle l d -> m ()
buildDataFile path rs h =
  withDb path $ \hnd ->
    forM_ rs $ \(r, oldr) -> do
      bs <- withDataLock h $ \(DataState dh _) -> readDocument dh oldr
      writeDocument r bs (hnd :: d)
