-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Utils
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the database.
----------------------------------------------------------------------------

module Database.Muesli.Utils
  ( tRecSize
  , writeLogTRec
  , pndTag
  , truTag
  , flsTag
  , cmpTag
  , rval
  , dbWordSize
  , checkLogSize
  , writeLogPos
  , readDocumentFromFile
  , writeDocument
  , mkNewId
  , findUnique
  , withMaster
  , withData
  , withGC
  , withUpdateMan
  , withMasterLock
  , withDataLock
  ) where

import           Control.Arrow            ((&&&))
import           Control.Concurrent       (putMVar, takeMVar)
import           Control.Exception        (Exception, bracket, bracketOnError)
import           Control.Monad            (forM_, unless)
import           Control.Monad.Trans      (MonadIO (liftIO))
import           Data.Bits                (finiteBitSize)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as B
import qualified Data.IntMap.Strict       as Map
import qualified Data.IntSet              as Set
import           Data.List                (filter, find, foldl')
import           Data.Maybe               (fromMaybe)
import           Data.Serialize           (encode)
import           Data.String              (IsString (..))
import qualified Database.Muesli.Cache    as Cache
import           Database.Muesli.IdSupply
import           Database.Muesli.Indexes
import           Database.Muesli.Internal
import           Database.Muesli.Types
import           GHC.TypeLits             (Symbol)
import qualified System.IO                as IO

rval :: Maybe (DocID a) -> Int
rval = fromIntegral . unDocID . fromMaybe maxBound

dbWordSize :: Int
dbWordSize = finiteBitSize (undefined :: DBWord) `div` 8

-- Brackets ------------------------------------------------------------------

withMasterLock :: MonadIO m => Handle -> (MasterState -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar . masterState $ unHandle h)
  (putMVar (masterState $ unHandle h))

withMaster :: MonadIO m => Handle -> (MasterState -> IO (MasterState, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar . masterState $ unHandle h)
  (putMVar (masterState $ unHandle h))
  (\m -> do
    (m', a) <- f m
    putMVar (masterState $ unHandle h) m'
    return a)

withDataLock :: MonadIO m => Handle -> (DataState -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar . dataState $ unHandle h)
  (putMVar (dataState $ unHandle h))

withData :: MonadIO m => Handle -> (DataState -> IO (DataState, a)) -> m a
withData h f = liftIO $ bracketOnError
  (takeMVar . dataState $ unHandle h)
  (putMVar (dataState $ unHandle h))
  (\d -> do
    (d', a) <- f d
    putMVar (dataState $ unHandle h) d'
    return a)

withUpdateMan :: MonadIO m => Handle -> (Bool -> IO (Bool, a)) -> m a
withUpdateMan h f = liftIO $ bracketOnError
  (takeMVar . updateMan $ unHandle h)
  (putMVar (updateMan $ unHandle h))
  (\kill -> do
    (kill', a) <- f kill
    putMVar (updateMan $ unHandle h) kill'
    return a)

withGC :: MonadIO m => Handle -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar . gcState $ unHandle h)
  (putMVar (gcState $ unHandle h))
  (\s -> do
    (s', a) <- f s
    putMVar (gcState $ unHandle h) s'
    return a)

-- IO ------------------------------------------------------------------------

writeWord :: MonadIO m => IO.Handle -> DBWord -> m ()
writeWord h w = liftIO . B.hPut h $ encode w

writeLogPos :: MonadIO m => IO.Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ IO.hSeek h IO.AbsoluteSeek 0
  liftIO $ B.hPut h $ encode p

checkLogSize :: MonadIO m => IO.Handle -> Int -> Int -> m Int
checkLogSize hnd osz pos =
  let bpos = dbWordSize * (pos + 1) in
  if bpos > osz then do
    let sz = max bpos $ osz + 4096
    liftIO . IO.hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

pndTag :: Int
pndTag = 0x70 -- ASCII 'p'

cmpTag :: Int
cmpTag = 0x63 -- ASCII 'c'

truTag :: Int
truTag = 0x54 -- ASCII 'T'

flsTag :: Int
flsTag = 0x46 -- ASCII 'F'

writeLogTRec :: MonadIO m => IO.Handle -> TRec -> m ()
writeLogTRec h t =
  case t of
    Pending doc -> do
      writeWord h $ fromIntegral pndTag
      writeWord h $ docTID doc
      writeWord h $ docID doc
      writeWord h $ docAddr doc
      writeWord h $ docSize doc
      writeWord h . fromIntegral $ if docDel doc then truTag else flsTag
      writeWordList $ (irefPID &&& irefVal) <$> docURefs doc
      writeWordList $ (irefPID &&& irefVal) <$> docIRefs doc
      writeWordList $ (drefPID &&& drefDID) <$> docDRefs doc
    Completed tid -> do
      writeWord h $ fromIntegral cmpTag
      writeWord h tid
  where writeWordList rs = do
          writeWord h . fromIntegral $ length rs
          forM_ rs $ \(pid, val) -> do
             writeWord h pid
             writeWord h val

tRecSize :: TRec -> Int
tRecSize r = case r of
  Pending dr  -> 9 + (2 * length (docURefs dr)) +
                     (2 * length (docIRefs dr)) +
                     (2 * length (docDRefs dr))
  Completed _ -> 2

readDocumentFromFile :: MonadIO m => IO.Handle -> DocRecord -> m ByteString
readDocumentFromFile hnd r = do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO . B.hGet hnd . fromIntegral $ docSize r

writeDocument :: MonadIO m => DocRecord -> ByteString -> IO.Handle -> m ()
writeDocument r bs hnd = unless (docDel r) $ do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO $ B.hPut hnd bs

mkNewId :: MonadIO m => Handle -> m TID
mkNewId h = withMaster h $ \m ->
  let (tid, s) = allocId (idSupply m) in
  return (m { idSupply = s }, tid)

findUnique :: IntValue -> IntValue -> [(DocRecord, a)] -> Maybe DID
findUnique p u rs = fmap docID . find findR $ map fst rs
  where findR = any (\i -> irefPID i == p && irefVal i == u) . docURefs
