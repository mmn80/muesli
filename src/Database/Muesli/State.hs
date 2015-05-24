-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.State
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
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
  , GapsIndex
  , PendingIndex
  , CompletedIndex
  , DataState (..)
  , GCState (..)
  , withMaster
  , withData
  , withGC
  , withUpdateMan
  , withMasterLock
  , withDataLock
  , DatabaseError (..)
  , mkNewTransactionId
  , mkNewDocumentKey
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

type MainIndex      = IntMap [LogRecord]
type SortIndex      = IntMap (IntMap IntSet)
type FilterIndex    = IntMap (IntMap SortIndex)
type UniqueIndex    = IntMap (IntMap Int)
type GapsIndex      = IntMap [DocAddress]
type PendingIndex   = Map TransactionId [(LogRecord, ByteString)]
type CompletedIndex = Map TransactionId [LogRecord]

data MasterState l = MasterState
  { logState  :: !l
  , topTid    :: !TransactionId
  , idSupply  :: !IdSupply
  , keepTrans :: !Bool
  , gaps      :: !GapsIndex
  , logPend   :: !PendingIndex
  , logComp   :: !CompletedIndex
  , mainIdx   :: !MainIndex
  , unqIdx    :: !UniqueIndex
  , sortIdx   :: !SortIndex
  , refIdx    :: !FilterIndex
  }

data DataState d = DataState
  { dataHandle :: !d
  , dataCache  :: !LRUCache
  }

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

mkNewTransactionId :: MonadIO m => Handle l d -> m TransactionId
mkNewTransactionId h = withMaster h $ \m ->
  let tid = topTid m + 1 in
  return (m { topTid = tid }, tid)

mkNewDocumentKey :: MonadIO m => Handle l d -> m DocumentKey
mkNewDocumentKey h = withMaster h $ \m ->
  let (tid, s) = Ids.alloc (idSupply m) in
  return (m { idSupply = s }, tid)

findUnique :: PropertyKey -> UniqueKey -> [(LogRecord, a)] -> Maybe DocumentKey
findUnique p u rs = fmap recDocumentKey . find findR $ map fst rs
  where findR = elem (p, u) . recUniques

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
