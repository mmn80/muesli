{-# LANGUAGE ScopedTypeVariables #-}

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
import           Data.ByteString       (ByteString, hGet, hPut)
import           Data.Word             (Word16, Word8)
import           Database.Muesli.State hiding (Handle)
import           Database.Muesli.Types
import           Foreign               (Storable, alloca, peek, sizeOf, with)
import           System.IO             (Handle, SeekMode (..), hFileSize,
                                        hGetBuf, hPutBuf, hSeek, hSetFileSize,
                                        hTell)

logSeek :: MonadIO m => MasterState -> m ()
logSeek m = liftIO . hSeek (logHandle m) AbsoluteSeek . fromIntegral $ logPos m

readBits :: forall a m. (Storable a, MonadIO m) => Handle -> m a
readBits h = liftIO . alloca $ \ptr ->
  liftIO $ hGetBuf h ptr (sizeOf (undefined :: a)) >> peek ptr

writeBits :: (Storable a, MonadIO m) => Handle -> a -> m ()
writeBits h w = liftIO . with w $ \ptr ->
  liftIO $ hPutBuf h ptr (sizeOf w)

readLogPos :: MonadIO m => Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ hFileSize h
  if sz >= fromIntegral (sizeOf (0 :: Addr)) then do
    liftIO $ hSeek h AbsoluteSeek 0
    w <- readBits h
    return (w, fromIntegral sz)
  else
    return (fromIntegral $ sizeOf (0 :: Addr), 0)

writeLogPos :: MonadIO m => Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ hSeek h AbsoluteSeek 0
  liftIO $ writeBits h p

checkLogSize :: MonadIO m => Handle -> Int -> Int -> m Int
checkLogSize hnd osz pos =
  if pos > osz then do
    let sz = max pos $ osz + 4096
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
  tag <- readBits h
  case tag of
    x | x == pndTag -> do
      tid <- readBits h
      did <- readBits h
      adr <- readBits h
      siz <- readBits h
      del <- readBits h
      dlb <- case del of
               y | y == truTag -> return True
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
      tid <- readBits h
      return $ Completed tid
    _ -> logError h $ showString "Pending ('p') or Completed ('c') tag expected but " .
           shows tag . showString " found."
  where readWordList con = do
          sz <- readBits h
          replicateM (fromIntegral (sz :: Word16)) $ do
            pid <- readBits h
            val <- readBits h
            return $ con pid val

writeLogTRec :: MonadIO m => Handle -> TRec -> m ()
writeLogTRec h t =
  case t of
    Pending doc -> do
      writeBits h pndTag
      writeBits h $ docTID doc
      writeBits h $ docID doc
      writeBits h $ docAddr doc
      writeBits h $ docSize doc
      writeBits h $ if docDel doc then truTag else flsTag
      writeWordList $ (irefPID &&& irefVal) <$> docURefs doc
      writeWordList $ (irefPID &&& irefVal) <$> docIRefs doc
      writeWordList $ (drefPID &&& drefDID) <$> docDRefs doc
    Completed tid -> do
      writeBits h cmpTag
      writeBits h tid
  where writeWordList rs = do
          writeBits h (fromIntegral (length rs) :: Word16)
          forM_ rs $ \(pid, val) -> do
             writeBits h pid
             writeBits h val

pndTag :: Word8
pndTag = 0x70 -- ASCII 'p'

cmpTag :: Word8
cmpTag = 0x63 -- ASCII 'c'

truTag :: Word8
truTag = 0x54 -- ASCII 'T'

flsTag :: Word8
flsTag = 0x46 -- ASCII 'F'

readDocument :: MonadIO m => Handle -> DocRecord -> m ByteString
readDocument hnd r = do
  liftIO . hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
  liftIO . hGet hnd . fromIntegral $ docSize r

writeDocument :: MonadIO m => DocRecord -> ByteString -> Handle -> m ()
writeDocument r bs hnd = unless (docDel r) $ do
  liftIO . hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
  liftIO $ hPut hnd bs

logError :: MonadIO m => Handle -> ShowS -> m a
logError h err = do
  pos <- liftIO $ hTell h
  liftIO . throw . LogParseError (fromIntegral pos) . showString
    "Corrupted log. " $ err ""
