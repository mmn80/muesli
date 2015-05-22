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
  ( Handle (..)
  , DBState (..)
  , MasterState (..)
  , MainIndex
  , SortIndex
  , FilterIndex
  , UniqueIndex
  , Gaps
  , DataState (..)
  , GCState (..)
  , withMaster
  , withData
  , withGC
  , withUpdateMan
  , withMasterLock
  , withDataLock
  , DatabaseError (..)
  , DocRecord (..)
  , DocReference (..)
  , IntReference (..)
  , TRec (..)
  , isPending
  , fromPending
  , tRecSize
  , Addr
  , Size
  , TID
  , DID
  , mkNewId
  , reserveIdsRec
  , findUnique
  ) where

import           Control.Concurrent       (MVar, putMVar, takeMVar)
import           Control.Exception        (bracket, bracketOnError)
import           Control.Monad.Trans      (MonadIO (liftIO))
import           Data.ByteString          (ByteString)
import           Data.IntMap.Strict       (IntMap)
import           Data.IntSet              (IntSet)
import           Data.List                (find)
import           Database.Muesli.Cache    (LRUCache)
import           Database.Muesli.IdSupply (IdSupply)
import qualified Database.Muesli.IdSupply as Ids
import           Database.Muesli.Types
import           Foreign                  (sizeOf)
import qualified System.IO                as IO

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  showsPrec p = showsPrec p . logFilePath . unHandle

type Addr   = DBWord
type TID    = DBWord
type DID    = DBWord
type UnqVal = DBWord
type Size   = DBWord
type PropID = DBWord

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , masterState  :: MVar MasterState
  , dataState    :: MVar DataState
  , updateMan    :: MVar Bool
  , gcState      :: MVar GCState
  }

instance Eq DBState where
  s == s' = logFilePath s == logFilePath s'

type MainIndex = IntMap [DocRecord]

type SortIndex = IntMap (IntMap IntSet)

type FilterIndex = IntMap (IntMap SortIndex)

type UniqueIndex = IntMap (IntMap Int)

type Gaps = IntMap [Addr]

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: !Addr
  , logSize   :: !Size
  , idSupply  :: !IdSupply
  , keepTrans :: !Bool
  , gaps      :: !Gaps
  , logPend   :: !(IntMap [(DocRecord, ByteString)])
  , logComp   :: !(IntMap [DocRecord])
  , mainIdx   :: !MainIndex
  , unqIdx    :: !UniqueIndex
  , intIdx    :: !SortIndex
  , refIdx    :: !FilterIndex
  }

data DataState = DataState
  { dataHandle :: IO.Handle
  , dataCache  :: !LRUCache
  }

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

data DocRecord = DocRecord
  { docID    :: !DID
  , docTID   :: !TID
  , docURefs :: ![IntReference]
  , docIRefs :: ![IntReference]
  , docDRefs :: ![DocReference]
  , docAddr  :: !Addr
  , docSize  :: !Size
  , docDel   :: !Bool
  } deriving (Show)

data DocReference = DocReference
  { drefPID :: !PropID
  , drefDID :: !DID
  } deriving (Show)

data IntReference = IntReference
  { irefPID :: !PropID
  , irefVal :: !DBWord
  } deriving (Show)

data TRec = Pending DocRecord | Completed TID
  deriving (Show)

isPending :: TRec -> Bool
isPending (Pending _)   = True
isPending (Completed _) = False

fromPending :: TRec -> DocRecord
fromPending (Pending r) = r

tRecSize :: TRec -> Int
tRecSize r = case r of
  Pending dr  -> 8 + ws * (4 + (2 * length (docURefs dr)) +
                               (2 * length (docIRefs dr)) +
                               (2 * length (docDRefs dr)))
  Completed _ -> 1 + ws
  where ws = sizeOf (0 :: DBWord)

mkNewId :: MonadIO m => Handle -> m TID
mkNewId h = withMaster h $ \m ->
  let (tid, s) = Ids.alloc (idSupply m) in
  return (m { idSupply = s }, tid)

reserveIdsRec :: IdSupply -> DocRecord -> IdSupply
reserveIdsRec s r = Ids.reserve (docTID r) $ Ids.reserve (docID r) s

findUnique :: PropID -> UnqVal -> [(DocRecord, a)] -> Maybe DID
findUnique p u rs = fmap docID . find findR $ map fst rs
  where findR = any (\i -> irefPID i == p && irefVal i == u) . docURefs

withMasterLock :: MonadIO m => Handle -> (MasterState -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)

withMaster :: MonadIO m => Handle -> (MasterState -> IO (MasterState, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar . masterState $ unHandle h)
  (putMVar . masterState $ unHandle h)
  (\m -> do
    (m', a) <- f m
    putMVar (masterState $ unHandle h) m'
    return a)

withDataLock :: MonadIO m => Handle -> (DataState -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)

withData :: MonadIO m => Handle -> (DataState -> IO (DataState, a)) -> m a
withData h f = liftIO $ bracketOnError
  (takeMVar . dataState $ unHandle h)
  (putMVar . dataState $ unHandle h)
  (\d -> do
    (d', a) <- f d
    putMVar (dataState $ unHandle h) d'
    return a)

withUpdateMan :: MonadIO m => Handle -> (Bool -> IO (Bool, a)) -> m a
withUpdateMan h f = liftIO $ bracketOnError
  (takeMVar . updateMan $ unHandle h)
  (putMVar . updateMan $ unHandle h)
  (\kill -> do
    (kill', a) <- f kill
    putMVar (updateMan $ unHandle h) kill'
    return a)

withGC :: MonadIO m => Handle -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar . gcState $ unHandle h)
  (putMVar . gcState $ unHandle h)
  (\s -> do
    (s', a) <- f s
    putMVar (gcState $ unHandle h) s'
    return a)
