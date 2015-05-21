-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.IO
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Log & data file IO functions.
----------------------------------------------------------------------------

module Database.Muesli.IO
  ( logSeek
  , readLogPos
  , writeLogPos
  , checkLogSize
  , readLogTRec
  , writeLogTRec
  , checkDataSize
  , readDocument
  , writeDocument
  , tRecSize
  , logError
  ) where

import           Control.Arrow         ((&&&))
import           Control.Exception     (throw)
import           Control.Monad         (forM_, replicateM, unless)
import           Control.Monad.Trans   (MonadIO (liftIO))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import           Data.Serialize        (decode, encode)
import           Database.Muesli.State hiding (Handle)
import           Database.Muesli.Types
import           System.IO             (Handle, SeekMode (..), hFileSize, hSeek,
                                        hSetFileSize, hTell)

logSeek :: MonadIO m => MasterState -> m ()
logSeek m = liftIO $ hSeek h AbsoluteSeek p
  where h = logHandle m
        p = fromIntegral $ dbWordSize * fromIntegral (1 + logPos m)

readWord :: MonadIO m => Handle -> m DBWord
readWord h = do
  bs <- liftIO $ B.hGet h dbWordSize
  either (logError h . showString) return (decode bs)

writeWord :: MonadIO m => Handle -> DBWord -> m ()
writeWord h w = liftIO . B.hPut h $ encode w

readLogPos :: MonadIO m => Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ hFileSize h
  if sz >= fromIntegral dbWordSize then do
    liftIO $ hSeek h AbsoluteSeek 0
    w <- readWord h
    return (w, fromIntegral sz)
  else
    return (0, 0)

writeLogPos :: MonadIO m => Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ hSeek h AbsoluteSeek 0
  liftIO $ B.hPut h $ encode p

checkLogSize :: MonadIO m => Handle -> Int -> Int -> m Int
checkLogSize hnd osz pos =
  let bpos = dbWordSize * (pos + 1) in
  if bpos > osz then do
    let sz = max bpos $ osz + 4096
    liftIO . hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

checkDataSize :: MonadIO m => Handle -> Int -> m Int
checkDataSize hnd szi = do
  let sz = fromIntegral szi
  osz <- liftIO $ hFileSize hnd
  if osz < sz then do
    let nsz = max sz $ osz + 4096
    liftIO $ hSetFileSize hnd nsz
    return $ fromIntegral nsz
  else return $ fromIntegral osz

readLogTRec :: MonadIO m => Handle -> m TRec
readLogTRec h = do
  tag <- readWord h
  case fromIntegral tag of
    x | x == pndTag -> do
      tid <- readWord h
      did <- readWord h
      adr <- readWord h
      siz <- readWord h
      del <- readWord h
      dlb <- case fromIntegral del of
               y | y == truTag  -> return True
               y | y == flsTag -> return False
               _ -> logError h $
                      showString "True ('T') or False ('F') tag expected but " .
                      shows del . showString " found."
      us <- readWordList IntReference
      is <- readWordList IntReference
      ds <- readWordList DocReference
      return $ Pending DocRecord { docID    = did
                                 , docTID   = tid
                                 , docURefs = us
                                 , docIRefs = is
                                 , docDRefs = ds
                                 , docAddr  = adr
                                 , docSize  = siz
                                 , docDel   = dlb
                                 }
    x | x == cmpTag -> do
      tid <- readWord h
      return $ Completed tid
    _ -> logError h $ showString "Pending ('p') or Completed ('c') tag expected but " .
           shows tag . showString " found."
  where readWordList con = do
          sz <- readWord h
          replicateM (fromIntegral sz) $ do
            pid <- readWord h
            val <- readWord h
            return $ con pid val

writeLogTRec :: MonadIO m => Handle -> TRec -> m ()
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

pndTag :: Int
pndTag = 0x70 -- ASCII 'p'

cmpTag :: Int
cmpTag = 0x63 -- ASCII 'c'

truTag :: Int
truTag = 0x54 -- ASCII 'T'

flsTag :: Int
flsTag = 0x46 -- ASCII 'F'

readDocument :: MonadIO m => Handle -> DocRecord -> m ByteString
readDocument hnd r = do
  liftIO . hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
  liftIO . B.hGet hnd . fromIntegral $ docSize r

writeDocument :: MonadIO m => DocRecord -> ByteString -> Handle -> m ()
writeDocument r bs hnd = unless (docDel r) $ do
  liftIO . hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
  liftIO $ B.hPut hnd bs

logError :: MonadIO m => Handle -> ShowS -> m a
logError h err = do
  pos <- liftIO $ hTell h
  liftIO . throw . LogParseError (fromIntegral pos) . showString
    "Corrupted log. " $ err ""
