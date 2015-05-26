{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Commit
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- 'Transaction' evaluation inside 'MonadIO'.
----------------------------------------------------------------------------

module Database.Muesli.Commit
  ( Transaction (..)
  , TransactionState (..)
  , runQuery
  , TransactionAbort (..)
  , commitThread
  ) where

import           Control.Concurrent        (threadDelay)
import           Control.Exception         (Exception)
import           Control.Monad             (forM_, liftM, unless, when)
import           Control.Monad.State       (StateT)
import qualified Control.Monad.State       as S
import           Control.Monad.Trans       (MonadIO (liftIO))
import           Data.ByteString           (ByteString)
import           Data.Function             (on)
import qualified Data.IntMap.Strict        as IntMap
import           Data.List                 (foldl')
import qualified Data.List                 as L
import qualified Data.Map.Strict           as Map
import           Data.Maybe                (fromMaybe)
import qualified Database.Muesli.Allocator as GapsIndex
import qualified Database.Muesli.Cache     as Cache
import           Database.Muesli.Indexes
import           Database.Muesli.State
import           Database.Muesli.Types
import           Prelude                   hiding (filter, lookup)

-- | Abstract monad for writing and evaluating queries under ACID semantics.
--
-- The @l@ parameter stands for a 'LogState' backend, @m@ is a 'MonadIO' that gets
-- lifted, so users can run arbitrary IO inside queries, and @a@ is the result.
newtype Transaction l m a = Transaction
  { unTransaction :: StateT (TransactionState l) m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction l m) where
  liftIO = Transaction . liftIO

-- | State held inside a 'Transaction'.
--
-- Note: The 'Transaction' type is internally 'StateT' ('TransactionState' l) m a.
data TransactionState l = TransactionState
  { transHandle     :: !(Handle l)
-- | Allocated by 'runQuery' with 'mkNewTransactionId' (auto-incremental)
  , transId         :: !TransactionId
-- | Accumulates all documents' keys that have been read as part of the
-- transaction. It is considered that all updates in the transaction may depend
-- of these, so the transaction is aborted if concurrent transactions update
-- any of them.
  , transReadList   :: ![DocumentKey]
-- | Accumulates all updated or deleted documents' keys.
  , transUpdateList :: ![(LogRecord, ByteString)]
  }

-- | Error returned by 'runQuery' for aborted transactions.
--
-- It is an instance of 'Exception' solely for user convenience,
-- as the database never throws it.
data TransactionAbort
  -- | Returned when trying to update an 'Unique' field with a preexisting value.
  = AbortUnique String
  -- | Returned when there is a conflict with concurrent transactions.
  -- This happenes when the 'transUpdateList' of transactions in 'logPend' or
  -- 'logComp' with 'transId' > then our own 'transId' has any keys that we also
  -- have in either 'transReadList', or 'transUpdateList'.
  --
  -- The second part could be relaxed in the future based on a user policy,
  -- since overwriting updates are sometimes acceptable.
  | AbortConflict String
  -- | Returned when  trying to delete a document that is still referenced by
  -- other documents.
  --
  -- TODO: Current implementation is not completely safe in this regard, as updates
  -- should also be checked. The reason is that the check is performed on indexes
  -- and on the pending log. So there is a small window in which it is possible
  -- for a concurrent transaction to update a record deleted by the current one,
  -- before adding it to the pending log, without any error.
  | AbortDelete String
  deriving (Show)

instance Exception TransactionAbort

-- | 'Transaction' evaluation inside a 'MonadIO'.
--
-- Lookups are executed directly, targeting a specific version ('TransactionId')
-- , while the keys of both read and written documents are collected in the
-- 'TransactionState'.
--
-- At the end various consistency checks are performed, and the transaction is
-- aborted in case any fails. See 'TransactionAbort' for details.
--
-- Otherwise, under master lock, space in the data (abstract) file is allocated
-- with 'GapsIndex.alloc', and the transaction records are written in the
-- 'logPend', and also in the log file, with 'logAppend'.
--
-- Writing the serialized data to the data file, updating indexes and completing
-- the transaction is then left for the 'commitThread'.
-- Note that transactions are only durable after 'commitThread' finishes.
-- In the future we may add a blocking version of 'runQuery'.
runQuery :: (MonadIO m, LogState l) => Handle l -> Transaction l m a ->
             m (Either TransactionAbort a)
runQuery h (Transaction t) = do
  tid <- mkNewTransactionId h
  (a, q, u) <- runUserCode tid
  if null u then return $ Right a
  else withMaster h $ \m ->
    if | not $ checkUnique u (unqIdx m) (logPend m) -> return (m, Left $
           AbortUnique "Transaction aborted: uniqueness check failed.")
       | not $ checkConflict tid (logPend m) (logComp m) q u -> return (m, Left $
           AbortConflict "Transaction aborted: conflict with concurrent transactions.")
       | not $ checkDelete m u -> return (m, Left $
           AbortDelete "Document cannot be deleted. Other documents still point to it.")
       | otherwise -> do
           let (ts, gs) = L.foldl' allocFold ([], gaps m) u
           st <- logAppend (logState m) (Pending . fst <$> ts)
           let m' = m { logState = st
                      , gaps     = gs
                      , logPend  = Map.insert tid ts $ logPend m
                      }
           return (m', Right a)
  where
    runUserCode tid = do
      (a, TransactionState _ _ q u) <- S.runStateT t
        TransactionState
          { transHandle     = h
          , transId         = tid
          , transReadList   = []
          , transUpdateList = []
          }
      let u' = L.nubBy ((==) `on` recDocumentKey . fst) u
      return (a, q, u')

    checkUnique u idx logp = all ck u
      where ck (d, _) = recDeleted d || all (cku $ recDocumentKey d) (recUniques d)
            cku did (pid, val) =
              cku' did (liftM fromIntegral $ IntMap.lookup (fromIntegral pid) idx >>=
                                             IntMap.lookup (fromIntegral val)) &&
              cku' did (findUnique pid val (concat $ Map.elems logp))
            cku' did = maybe True (== did)

    checkConflict tid logp logc q u = ck qs && ck us
      where
        us = (\(d,_) -> (fromIntegral (recDocumentKey d), ())) <$> u
        qs = (\k -> (fromIntegral k, ())) <$> q
        ck lst = Map.null (Map.intersection newPs ml) &&
                 Map.null (Map.intersection newCs ml)
          where ml = Map.fromList lst
        newPs = snd $ Map.split tid logp
        newCs = snd $ Map.split tid logc

    checkDelete m u = all ck . L.filter recDeleted $ map fst u
      where ck d = all (ckEmpty $ fromIntegral did) lst && all (ckPnd did) rs
              where did = recDocumentKey d
            lst = IntMap.elems $ refIdx m
            ckEmpty did idx =
              case IntMap.lookup did idx of
                Nothing -> True
                Just ss -> all null $ IntMap.elems ss
            rs = L.filter (not . recDeleted) . map fst . concat . Map.elems $ logPend m
            ckPnd did r = not . any ((did ==) . snd) $ recReferences r

    allocFold (ts, gs) (r, bs) =
      if recDeleted r then ((r, bs):ts, gs)
      else ((r', bs):ts, gs')
        where (a, gs') = GapsIndex.alloc gs $ recSize r
              r' = r { recAddress = a }

-- | Code for the commit thread forked by 'Database.Muesli.Handle.open'.
--
-- It periodically checks for new records in the 'logPend', and processes them,
-- by adding a 'Completed' record to the log file with 'logAppend', and
-- updating indexes with 'updateMainIdx', 'updateRefIdx', 'updateSortIdx' and
-- 'updateUnqIdx', after writing (without master lock) the serialized documents
-- in the data file with 'writeDocument'.
-- It also moves the records from 'logPend' to 'logComp'.
commitThread :: LogState l => Handle l -> Bool -> IO ()
commitThread h w = do
  (kill, wait) <- withCommitSgn h $ \kill -> do
    wait <- if kill then return True else do
      when w . threadDelay . commitDelay $ unHandle h
      withMasterLock h $ \m ->
        let lgp = logPend m in
        if null lgp then return Nothing
        else return . Just $ Map.findMin lgp
      >>=
      maybe (return True) (\(tid, rs) -> do
        withData h $ \(DataState hnd cache) -> do
          forM_ rs $ \(r, bs) -> writeDocument r bs hnd
          let cache' = foldl' (\c (r, _) -> Cache.delete (fromIntegral $ recAddress r) c)
                         cache rs
          return (DataState hnd cache', ())
        withMaster h $ \m -> do
          let rs' = fst <$> rs
          let trec = Completed tid
          st <- logAppend (logState m) [trec]
          let (lgp, lgc) = updateLog tid (keepTrans m) (logPend m) (logComp m)
          let m' = m { logState = st
                     , logPend  = lgp
                     , logComp  = lgc
                     , mainIdx  = updateMainIdx (mainIdx m) rs'
                     , unqIdx   = updateUnqIdx  (unqIdx m)  rs'
                     , sortIdx  = updateSortIdx (sortIdx m) rs'
                     , refIdx   = updateRefIdx  (refIdx m)  rs'
                     }
          return (m', null lgp))
    return (kill, (kill, wait))
  unless kill $ commitThread h wait
  where updateLog tid keep lgp lgc = (lgp', lgc')
          where ors  = map fst . fromMaybe [] $ Map.lookup tid lgp
                lgp' = Map.delete tid lgp
                lc   = if not keep && null lgp'
                       then Map.empty else lgc
                lgc' = if keep || not (null lgp')
                       then Map.insert tid ors lc else lc
