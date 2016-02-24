-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.State
-- Copyright   : (c) 2015-16 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- The internal state of the database.
----------------------------------------------------------------------------

module Database.Muesli.State
  ( module Database.Muesli.Backend.Types
  -- * Database state
  , Handle (..)
  , DBState (..)
  , MasterState (..)
  , DataState (..)
  , GCState (..)
  -- ** Allocation table
  , MainIndex
  , GapsIndex
  -- ** Inverted indexes
  , SortIndex
  , FilterIndex
  , UniqueIndex
  -- ** Transaction log
  , PendingIndex
  , CompletedIndex
  -- * Bracket functions
  , withMasterLock
  , withMaster
  , withDataLock
  , withData
  , withGC
  , withCommitSgn
  -- * Utilities
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

-- | Handle used for database management operations.
--
-- The @l@ parameter stands for a 'LogState' backend.
newtype Handle l = Handle { unHandle :: DBState l } deriving (Eq)

instance Show (Handle l) where
  showsPrec p = showsPrec p . logDbPath . unHandle

-- | The internal state of the database.
data DBState l = DBState
  { logDbPath   :: DbPath
  , dataDbPath  :: DbPath
  , commitDelay :: Int
  , masterState :: MVar (MasterState l)
  , dataState   :: MVar (DataState l)
  , commitSgn   :: MVar Bool    -- ^ Used to send the kill signal to the
-- 'Database.Muesli.Commit.commitThread'
  , gcState     :: MVar GCState -- ^ Used to communicate with the
-- 'Database.Muesli.GC.gcThread'
  }

instance Eq (DBState l) where
  s == s' = logDbPath s == logDbPath s'

-- | Type of the allocation table of the database.
--
-- The key of the 'IntMap' is the 'DocumentKey' of the corresponding document.
type MainIndex      = IntMap [LogRecord]

-- | Type of the sort index, which an
-- <https://en.wikipedia.org/wiki/Inverted_index inverted index>.
--
-- First key is 'PropertyKey', second is 'SortableKey', and the 'IntSet'
-- contains all 'DocumentKey's for the documents whose fields have the values
-- specified by the first two keys.
type SortIndex      = IntMap (IntMap IntSet)

-- | Type of the filter index, which is a 2-level nested
-- <https://en.wikipedia.org/wiki/Inverted_index inverted index>.
--
-- First key is the 'PropertyKey' for the filter field, second is the
-- 'DocumentKey' of the filter field value, and then an entire 'SortIndex'
-- containing the ordered subset for all sortable fields.
type FilterIndex    = IntMap (IntMap SortIndex)

-- | Type of the unique index, which is a simpler
-- <https://en.wikipedia.org/wiki/Inverted_index inverted index>.
--
-- First key is 'PropertyKey', second is the 'UniqueKey', and then the unique
-- 'DocumentKey' corresponding to that 'UniqueKey'.
type UniqueIndex    = IntMap (IntMap Int)

-- | A map from gap size to a list of addresses where gaps of that size start.
type GapsIndex      = Map DocSize [DocAddress]

-- | The type of the pending transaction log.
--
-- 'Database.Muesli.Query.update' serializes the document before adding it to
-- 'Database.Muesli.Commit.transUpdateList', and later
-- 'Database.Muesli.Commit.commitThread' moves it in the pending log.
type PendingIndex   = Map TransactionId [(LogRecord, ByteString)]

-- | The type of the completed transaction log.
type CompletedIndex = Map TransactionId [LogRecord]

-- | Type of the master state, holding all indexes.
--
-- When talking about /master lock/ in other parts, we mean taking the
-- 'masterState' 'MVar'.
data MasterState l = MasterState
  {
  -- | The 'LogState' backend.
    logState  :: !l
  -- | Auto-incremented global value for generating 'TransactionId's.
  , topTid    :: !TransactionId
  -- | Suppy for generating unique 'DocumentKey's.
  , idSupply  :: !IdSupply
  -- | This flag is set during GC, so that 'logComp' is not cleared as normal,
  -- since we need at the end of the GC to find the transactions that completed
  -- in the mean time, and we don't want to do log file IO for that.
  , keepTrans :: !Bool
  , gaps      :: !GapsIndex
  , logPend   :: !PendingIndex
  , logComp   :: !CompletedIndex
  , mainIdx   :: !MainIndex
  , unqIdx    :: !UniqueIndex
  , sortIdx   :: !SortIndex
  , refIdx    :: !FilterIndex
  }

-- | The state coresponding to the data file.
data DataState l = DataState
  { dataHandle :: !(DataHandleOf l)
  , dataCache  :: !LRUCache
  }

-- | Type for the state of the GC thread used for messaging.
data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

-- | Generates a new 'TransactionId' by incrementing the 'topTid' under
-- master lock.
mkNewTransactionId :: MonadIO m => Handle l -> m TransactionId
mkNewTransactionId h = withMaster h $ \m ->
  let tid = topTid m + 1 in
  return (m { topTid = tid }, tid)

-- | Generates a new 'DocumentKey' by calling 'Ids.alloc' under master lock.
mkNewDocumentKey :: MonadIO m => Handle l -> m DocumentKey
mkNewDocumentKey h = withMaster h $ \m ->
  let (tid, s) = Ids.alloc (idSupply m) in
  return (m { idSupply = s }, tid)

-- | Utility function for searching into a list of 'LogRecord's and into their
-- 'recUniques' for a particular 'UniqueKey'.
findUnique :: PropertyKey -> UniqueKey -> [(LogRecord, a)] -> Maybe DocumentKey
findUnique p u rs = fmap recDocumentKey . find findR $ map fst rs
  where findR = elem (p, u) . recUniques

-- | Standard 'bracket' function for the 'masterState' lock.
withMasterLock :: MonadIO m => Handle l -> (MasterState l -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)

-- | Standard 'bracket' function for the 'masterState' lock that also allows
-- updating the 'MasterState'.
withMaster :: MonadIO m => Handle l -> (MasterState l -> IO (MasterState l, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)
  (\m -> do
    (m', a) <- f m
    putMVar (masterState $ unHandle h) m'
    return a)

-- | Standard 'bracket' function for the 'dataState' lock.
withDataLock :: MonadIO m => Handle l -> (DataState l -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)

-- | Standard 'bracket' function for the 'dataState' lock that also allows
-- updating the 'DataState'.
withData :: MonadIO m => Handle l -> (DataState l -> IO (DataState l, a)) -> m a
withData h f = liftIO $ bracketOnError
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)
  (\d -> do
    (d', a) <- f d
    putMVar (dataState $ unHandle h) d'
    return a)

-- | Standard 'bracket' function for the 'commitSgn' lock that also allows
-- updating the 'Bool'.
withCommitSgn :: MonadIO m => Handle l -> (Bool -> IO (Bool, a)) -> m a
withCommitSgn h f = liftIO $ bracketOnError
  (takeMVar . commitSgn $ unHandle h)
  (putMVar . commitSgn $ unHandle h)
  (\kill -> do
    (kill', a) <- f kill
    putMVar (commitSgn $ unHandle h) kill'
    return a)

-- | Standard 'bracket' function for the 'gcState' lock that also allows
-- updating the 'GCState'.
withGC :: MonadIO m => Handle l -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar . gcState $ unHandle h)
  (putMVar . gcState $ unHandle h)
  (\s -> do
    (s', a) <- f s
    putMVar (gcState $ unHandle h) s'
    return a)
