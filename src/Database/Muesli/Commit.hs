{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}

-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Commit
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides transaction commit.
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
import qualified Data.IntMap.Strict        as Map
import           Data.List                 (foldl')
import qualified Data.List                 as L
import           Data.Maybe                (fromMaybe)
import qualified Database.Muesli.Allocator as Gaps
import qualified Database.Muesli.Cache     as Cache
import           Database.Muesli.Indexes
import           Database.Muesli.IO
import           Database.Muesli.State
import           Database.Muesli.Types
import           Prelude                   hiding (filter, lookup)

newtype Transaction m a = Transaction
  { unTransaction :: StateT TransactionState m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction m) where
  liftIO = Transaction . liftIO

data TransactionState = TransactionState
  { transHandle     :: Handle
  , transTID        :: !TID
  , transReadList   :: ![DID]
  , transUpdateList :: ![(DocRecord, ByteString)]
  }

data TransactionAbort = AbortUnique String
                      | AbortConflict String
                      | AbortDelete String
  deriving (Show)

instance Exception TransactionAbort

runQuery :: MonadIO m => Handle -> Transaction m a ->
            m (Either TransactionAbort a)
runQuery h (Transaction t) = do
  tid <- mkNewId h
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
           let tsz = sum $ (tRecSize . Pending . fst) <$> u
           let pos = fromIntegral (logPos m) + tsz
           lsz <- checkLogSize (logHandle m) (fromIntegral (logSize m)) pos
           let (ts, gs) = L.foldl' allocFold ([], gaps m) u
           let m' = m { logPos  = fromIntegral pos
                      , logSize = fromIntegral lsz
                      , gaps    = gs
                      , logPend = Map.insert (fromIntegral tid) ts $ logPend m
                      }
           writeTransactions m ts
           writeLogPos (logHandle m) $ fromIntegral pos
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
              cku' did (liftM fromIntegral $ Map.lookup (fromIntegral pid) idx >>=
                                             Map.lookup (fromIntegral val)) &&
              cku' did (findUnique pid val (concat $ Map.elems logp))
            cku' did = maybe True (== did)

    checkConflict tid logp logc q u = ck qs && ck us
      where
        us = (\(d,_) -> (fromIntegral (docID d), ())) <$> u
        qs = (\k -> (fromIntegral k, ())) <$> q
        ck lst = Map.null (Map.intersection newPs ml) &&
                 Map.null (Map.intersection newCs ml)
          where ml = Map.fromList lst
        newPs = snd $ Map.split (fromIntegral tid) logp
        newCs = snd $ Map.split (fromIntegral tid) logc

    checkDelete m u = all ck . L.filter docDel $ map fst u
      where ck d = all (ckEmpty $ fromIntegral did) lst && all (ckPnd did) rs
              where did = docID d
            lst = Map.elems $ refIdx m
            ckEmpty did idx =
              case Map.lookup did idx of
                Nothing -> True
                Just ss -> all null $ Map.elems ss
            rs = L.filter (not . docDel) . map fst . concat . Map.elems $ logPend m
            ckPnd did r = not . any ((did ==) . drefDID) $ docDRefs r

    allocFold (ts, gs) (r, bs) =
      if docDel r then ((r, bs):ts, gs)
      else ((r', bs):ts, gs')
        where (a, gs') = Gaps.alloc gs $ docSize r
              r' = r { docAddr = a }

    writeTransactions m ts = do
      logSeek m
      forM_ ts $ \(r, _) ->
        writeLogTRec (logHandle m) $ Pending r

updateManThread :: Handle -> Bool -> IO ()
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
          let maxAddr = maximum $ (\r -> docAddr r + docSize r) . fst <$> rs
          checkDataSize hnd . fromIntegral $ maxAddr + 1
          forM_ rs $ \(r, bs) -> writeDocument r bs hnd
          let cache' = foldl' (\c (r, _) -> Cache.delete (fromIntegral $ docAddr r) c)
                         cache rs
          return (DataState hnd cache', ())
        withMaster h $ \m -> do
          let rs' = fst <$> rs
          let trec = Completed $ fromIntegral tid
          let pos = fromIntegral (logPos m) + tRecSize trec
          lsz <- checkLogSize (logHandle m) (fromIntegral (logSize m)) pos
          logSeek m
          let lh = logHandle m
          writeLogTRec lh trec
          writeLogPos lh $ fromIntegral pos
          let (lgp, lgc) = updateLog tid (keepTrans m) (logPend m) (logComp m)
          let m' = m { logPos  = fromIntegral pos
                     , logSize = fromIntegral lsz
                     , logPend = lgp
                     , logComp = lgc
                     , mainIdx = updateMainIdx (mainIdx m) rs'
                     , unqIdx  = updateUnqIdx (unqIdx m) rs'
                     , intIdx  = updateIntIdx (intIdx m) rs'
                     , refIdx  = updateRefIdx (refIdx m) rs'
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
