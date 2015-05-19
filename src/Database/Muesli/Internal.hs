-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Internal
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides internal state types.
----------------------------------------------------------------------------

module Database.Muesli.Internal
  ( Handle (..)
  , DBState (..)
  , MasterState (..)
  , SortIndex
  , FilterIndex
  , UniqueIndex
  , DataState (..)
  , GCState (..)
  , DatabaseError (..)
  , DocRecord (..)
  , DocReference (..)
  , IntReference (..)
  , TRec (..)
  , isPending
  , fromPending
  , Addr
  , Size
  , TID
  , DID
  , IntValue
  , PropID
  ) where

import           Control.Concurrent       (MVar)
import           Control.Monad.Trans      (MonadIO (liftIO))
import           Data.ByteString          (ByteString)
import           Data.IntMap.Strict       (IntMap)
import           Data.IntSet              (IntSet)
import           Database.Muesli.Cache    (LRUCache)
import           Database.Muesli.IdSupply (IdSupply)
import           Database.Muesli.Types
import qualified System.IO                as IO

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  showsPrec p = showsPrec p . logFilePath . unHandle

type Addr     = DBWord
type TID      = DBWord
type DID      = DBWord
type IntValue = DBWord
type Size     = DBWord
type PropID   = DBWord

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

type SortIndex = IntMap (IntMap IntSet)

type FilterIndex = IntMap (IntMap SortIndex)

type UniqueIndex = IntMap (IntMap Int)

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: !Addr
  , logSize   :: !Size
  , idSupply  :: !IdSupply
  , keepTrans :: !Bool
  , gaps      :: !(IntMap [Addr])
  , logPend   :: !(IntMap [(DocRecord, ByteString)])
  , logComp   :: !(IntMap [DocRecord])
  , mainIdx   :: !(IntMap [DocRecord])
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
  , irefVal :: !IntValue
  } deriving (Show)

data TRec = Pending DocRecord | Completed TID
  deriving (Show)

isPending (Pending _)   = True
isPending (Completed _) = False
fromPending (Pending r) = r
