-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.State
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- The internal state of the database.
----------------------------------------------------------------------------

module Database.Muesli.State
  ( module Database.Muesli.Backend.Types
  , Handle (..)
  , DBState (..)
  , MasterState (..)
  , MainIndex
  , SortIndex
  , FilterIndex
  , UniqueIndex
  , Gaps
  , LogPending
  , LogCompleted
  , DataState (..)
  , GCState (..)
  , withMaster
  , withData
  , withGC
  , withUpdateMan
  , withMasterLock
  , withDataLock
  , DatabaseError (..)
  , mkNewTransId
  , mkNewDocId
  , findUnique
  ) where

import           Control.Concurrent            (MVar, putMVar, takeMVar)
import           Control.Exception             (bracket, bracketOnError)
import           Control.Monad.Trans           (MonadIO (liftIO))
import           Data.ByteString               (ByteString)
import           Data.IntMap.Strict            (IntMap)
import           Data.IntSet                   (IntSet)
import           Data.List                     (find)
import           Data.Map.Strict               (Map)
import           Database.Muesli.Backend.Types
import           Database.Muesli.Cache         (LRUCache)
import           Database.Muesli.IdSupply      (IdSupply)
import qualified Database.Muesli.IdSupply      as Ids
import           Database.Muesli.Types

newtype Handle l d = Handle { unHandle :: DBState l d } deriving (Eq)

instance Show (Handle l d) where
  showsPrec p = showsPrec p . logFilePath . unHandle

data DBState l d = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , masterState  :: MVar (MasterState l)
  , dataState    :: MVar (DataState d)
  , updateMan    :: MVar Bool
  , gcState      :: MVar GCState
  }

instance Eq (DBState l d) where
  s == s' = logFilePath s == logFilePath s'

type MainIndex = IntMap [DocRecord]

type SortIndex = IntMap (IntMap IntSet)

type FilterIndex = IntMap (IntMap SortIndex)

type UniqueIndex = IntMap (IntMap Int)

type Gaps = IntMap [Addr]

type LogPending = Map TID [(DocRecord, ByteString)]

type LogCompleted = Map TID [DocRecord]

data MasterState l = MasterState
  { logState  :: !l
  , topTID    :: !TID
  , idSupply  :: !IdSupply
  , keepTrans :: !Bool
  , gaps      :: !Gaps
  , logPend   :: !LogPending
  , logComp   :: !LogCompleted
  , mainIdx   :: !MainIndex
  , unqIdx    :: !UniqueIndex
  , intIdx    :: !SortIndex
  , refIdx    :: !FilterIndex
  }

data DataState d = DataState
  { dataHandle :: !d
  , dataCache  :: !LRUCache
  }

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

mkNewTransId :: MonadIO m => Handle l d -> m TID
mkNewTransId h = withMaster h $ \m ->
  let tid = topTID m + 1 in
  return (m { topTID = tid }, tid)

mkNewDocId :: MonadIO m => Handle l d -> m DID
mkNewDocId h = withMaster h $ \m ->
  let (tid, s) = Ids.alloc (idSupply m) in
  return (m { idSupply = s }, tid)

findUnique :: PropID -> UnqVal -> [(DocRecord, a)] -> Maybe DID
findUnique p u rs = fmap docID . find findR $ map fst rs
  where findR = any (\i -> irefPID i == p && irefVal i == u) . docURefs

withMasterLock :: MonadIO m => Handle l d -> (MasterState l -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)

withMaster :: MonadIO m => Handle l d -> (MasterState l -> IO (MasterState l, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)
  (\m -> do
    (m', a) <- f m
    putMVar (masterState $ unHandle h) m'
    return a)

withDataLock :: MonadIO m => Handle l d -> (DataState d -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)

withData :: MonadIO m => Handle l d -> (DataState d -> IO (DataState d, a)) -> m a
withData h f = liftIO $ bracketOnError
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)
  (\d -> do
    (d', a) <- f d
    putMVar (dataState $ unHandle h) d'
    return a)

withUpdateMan :: MonadIO m => Handle l d -> (Bool -> IO (Bool, a)) -> m a
withUpdateMan h f = liftIO $ bracketOnError
  (takeMVar . updateMan $ unHandle h)
  (putMVar . updateMan $ unHandle h)
  (\kill -> do
    (kill', a) <- f kill
    putMVar (updateMan $ unHandle h) kill'
    return a)

withGC :: MonadIO m => Handle l d -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar . gcState $ unHandle h)
  (putMVar . gcState $ unHandle h)
  (\s -> do
    (s', a) <- f s
    putMVar (gcState $ unHandle h) s'
    return a)
