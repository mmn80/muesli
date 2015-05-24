{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Backend.Types
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Backend interface types and classes.
----------------------------------------------------------------------------

module Database.Muesli.Backend.Types
  ( TransactionId
  , PropertyKey
  , DocAddress
  , DocSize
  , LogRecord (..)
  , TransRecord (..)
  , DbPath
  , DbHandle (..)
  , LogState (..)
  , DataHandle (..)
  ) where

import           Control.Monad.Trans   (MonadIO)
import           Data.ByteString       (ByteString)
import           Data.Word             (Word64)
import           Database.Muesli.Types (DocumentKey, IxKey (..), SortableKey,
                                        UniqueKey)

type TransactionId = Word64
type PropertyKey   = IxKey
type DocAddress    = IxKey
type DocSize       = IxKey

data LogRecord = LogRecord
  { recTransactionId :: !TransactionId
  , recDocumentKey   :: !DocumentKey
  , recReferences    :: ![(PropertyKey, DocumentKey)]
  , recSortables     :: ![(PropertyKey, SortableKey)]
  , recUniques       :: ![(PropertyKey, UniqueKey)]
  , recAddress       :: !DocAddress
  , recSize          :: !DocSize
  , recDeleted       :: !Bool
  } deriving (Show)

data TransRecord = Pending LogRecord | Completed TransactionId
  deriving (Show)

type DbPath = String

class DbHandle a where
  openDb  :: MonadIO m => DbPath -> m a
  closeDb :: MonadIO m => a -> m ()
  withDb  :: MonadIO m => DbPath -> (a -> IO b) -> m b
  swapDb  :: MonadIO m => DbPath -> DbPath -> m a

class (Show a, DbHandle (LogHandle a)) => LogState a where
  type LogHandle a :: *
  logHandle :: a -> LogHandle a
  logInit   :: MonadIO m => LogHandle a -> m a
  logAppend :: MonadIO m => a -> [TransRecord] -> m a
  logRead   :: MonadIO m => a -> m (Maybe TransRecord)

class DbHandle a => DataHandle a where
  readDocument  :: MonadIO m => a -> LogRecord -> m ByteString
  writeDocument :: MonadIO m => LogRecord -> ByteString -> a -> m ()
