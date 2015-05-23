{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Commit
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Transaction commit procedure.
----------------------------------------------------------------------------

module Database.Muesli.Commit
  ( Transaction (..)
  , TransactionState (..)
  , runQuery
  , TransactionAbort (..)
  , updateManThread
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
import qualified Database.Muesli.Allocator as Gaps
import qualified Database.Muesli.Cache     as Cache
import           Database.Muesli.Indexes
import           Database.Muesli.State
import           Database.Muesli.Types
import           Prelude                   hiding (filter, lookup)

newtype Transaction l d m a = Transaction
  { unTransaction :: StateT (TransactionState l d) m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction l d m) where
  liftIO = Transaction . liftIO

data TransactionState l d = TransactionState
  { transHandle     :: Handle l d
  , transTID        :: !TID
  , transReadList   :: ![DID]
  , transUpdateList :: ![(DocRecord, ByteString)]
  }

data TransactionAbort = AbortUnique String
                      | AbortConflict String
                      | AbortDelete String
  deriving (Show)

instance Exception TransactionAbort

runQuery :: (MonadIO m, LogState l) => Handle l d -> Transaction l d m a ->
            m (Either TransactionAbort a)
runQuery h (Transaction t) = do
  tid <- mkNewTransId h
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
          , transTID        = tid
          , transReadList   = []
          , transUpdateList = []
          }
      let u' = L.nubBy ((==) `on` docID . fst) u
      return (a, q, u')

    checkUnique u idx logp = all ck u
      where ck (d, _) = docDel d || all (cku $ docID d) (docURefs d)
            cku did (IntReference pid val) =
              cku' did (liftM fromIntegral $ IntMap.lookup (fromIntegral pid) idx >>=
                                             IntMap.lookup (fromIntegral val)) &&
              cku' did (findUnique pid val (concat $ Map.elems logp))
            cku' did = maybe True (== did)

    checkConflict tid logp logc q u = ck qs && ck us
      where
        us = (\(d,_) -> (fromIntegral (docID d), ())) <$> u
        qs = (\k -> (fromIntegral k, ())) <$> q
        ck lst = Map.null (Map.intersection newPs ml) &&
                 Map.null (Map.intersection newCs ml)
          where ml = Map.fromList lst
        newPs = snd $ Map.split tid logp
        newCs = snd $ Map.split tid logc

    checkDelete m u = all ck . L.filter docDel $ map fst u
      where ck d = all (ckEmpty $ fromIntegral did) lst && all (ckPnd did) rs
              where did = docID d
            lst = IntMap.elems $ refIdx m
            ckEmpty did idx =
              case IntMap.lookup did idx of
                Nothing -> True
                Just ss -> all null $ IntMap.elems ss
            rs = L.filter (not . docDel) . map fst . concat . Map.elems $ logPend m
            ckPnd did r = not . any ((did ==) . drefDID) $ docDRefs r

    allocFold (ts, gs) (r, bs) =
      if docDel r then ((r, bs):ts, gs)
      else ((r', bs):ts, gs')
        where (a, gs') = Gaps.alloc gs $ docSize r
              r' = r { docAddr = a }

updateManThread :: (LogState l, DataHandle d) => Handle l d -> Bool -> IO ()
updateManThread h w = do
  (kill, wait) <- withUpdateMan h $ \kill -> do
    wait <- if kill then return True else do
      when w . threadDelay $ 100 * 1000
      withMasterLock h $ \m ->
        let lgp = logPend m in
        if null lgp then return Nothing
        else return . Just $ Map.findMin lgp
      >>=
      maybe (return True) (\(tid, rs) -> do
        withData h $ \(DataState hnd cache) -> do
          forM_ rs $ \(r, bs) -> writeDocument r bs hnd
          let cache' = foldl' (\c (r, _) -> Cache.delete (fromIntegral $ docAddr r) c)
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
                     , unqIdx   = updateUnqIdx (unqIdx m) rs'
                     , intIdx   = updateIntIdx (intIdx m) rs'
                     , refIdx   = updateRefIdx (refIdx m) rs'
                     }
          return (m', null lgp))
    return (kill, (kill, wait))
  unless kill $ updateManThread h wait
  where updateLog tid keep lgp lgc = (lgp', lgc')
          where ors  = map fst . fromMaybe [] $ Map.lookup tid lgp
                lgp' = Map.delete tid lgp
                lc   = if not keep && null lgp'
                       then Map.empty else lgc
                lgc' = if keep || not (null lgp')
                       then Map.insert tid ors lc else lc
