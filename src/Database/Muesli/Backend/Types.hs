{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Backend.Types
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Backend interface types and classes.
----------------------------------------------------------------------------

module Database.Muesli.Backend.Types
  ( TID
  , DID
  , PropID
  , UnqVal
  , Addr
  , Size
  , DocRecord (..)
  , DocReference (..)
  , IntReference (..)
  , TRec (..)
  , isPending
  , fromPending
  , DbPath
  , DbHandle (..)
  , LogState (..)
  , DataHandle (..)
  ) where

import           Control.Monad.Trans   (MonadIO)
import           Data.ByteString       (ByteString)
import           Data.Word             (Word64)
import           Database.Muesli.Types (IxKey (..))

type TID    = Word64
type DID    = IxKey
type UnqVal = IxKey
type PropID = IxKey
type Addr   = IxKey
type Size   = IxKey

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
  , irefVal :: !IxKey
  } deriving (Show)

data TRec = Pending DocRecord | Completed TID
  deriving (Show)

isPending :: TRec -> Bool
isPending (Pending _)   = True
isPending (Completed _) = False

fromPending :: TRec -> DocRecord
fromPending (Pending r) = r

type DbPath = String

class DbHandle a where
  openDb  :: MonadIO m => DbPath -> m a
  closeDb :: MonadIO m => a -> m ()
  withDb  :: MonadIO m => DbPath -> (a -> IO b) -> m b
  swapDb  :: MonadIO m => DbPath -> DbPath -> m a

class Show a => LogState a where
  type LogHandle a :: *
  logHandle :: DbHandle (LogHandle a) => a -> LogHandle a
  logInit   :: MonadIO m => LogHandle a -> m a
  logAppend :: MonadIO m => a -> [TRec] -> m a
  logRead   :: MonadIO m => a -> m (Maybe TRec)

class DbHandle a => DataHandle a where
  readDocument :: MonadIO m => a -> DocRecord -> m ByteString
  writeDocument :: MonadIO m => DocRecord -> ByteString -> a -> m ()
