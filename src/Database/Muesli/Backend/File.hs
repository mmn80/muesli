{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Backend.File
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Binary seekable file backend that uses 'Prelude' functions.
----------------------------------------------------------------------------

module Database.Muesli.Backend.File
  () where

import           Control.Arrow                 ((&&&))
import           Control.Exception             (throw)
import           Control.Monad                 (forM_, replicateM, unless, when)
import           Control.Monad.Trans           (MonadIO (liftIO))
import           Data.ByteString               (hGet, hPut)
import           Data.Maybe                    (fromMaybe)
import           Data.Word                     (Word16, Word8)
import           Database.Muesli.Backend.Types
import           Database.Muesli.Types
import           Foreign                       (Storable, alloca, peek, sizeOf,
                                                with)
import           System.Directory              (renameFile)
import           System.IO                     (BufferMode (..), Handle,
                                                IOMode (..), SeekMode (..),
                                                hClose, hFileSize, hGetBuf,
                                                hPutBuf, hSeek, hSetBuffering,
                                                hSetFileSize, hTell,
                                                openBinaryFile, withBinaryFile)

instance DbHandle Handle where
  openDb path = liftIO $ do
    hnd <- openBinaryFile path ReadWriteMode
    hSetBuffering hnd NoBuffering
    return hnd

  closeDb hnd = liftIO $ hClose hnd

  withDb path = liftIO . withBinaryFile path ReadWriteMode

  swapDb oldPath path = liftIO (renameFile path oldPath) >> openDb path

instance DataHandle Handle where
  readDocument hnd r = do
    liftIO . hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
    liftIO . hGet hnd . fromIntegral $ docSize r

  writeDocument r bs hnd = unless (docDel r) . liftIO $ do
    let sz = fromIntegral $ docAddr r + docSize r
    osz <- hFileSize hnd
    when (osz < sz) $ do
      let nsz = max sz $ osz + 4096
      hSetFileSize hnd nsz
    hSeek hnd AbsoluteSeek . fromIntegral $ docAddr r
    hPut hnd bs

data FileLog = FileLog
  { flogHandle :: Handle
  , flogPos    :: Addr
  , flogSize   :: Size
  } deriving (Show)

instance LogState FileLog where
  type LogHandle FileLog = Handle

  logHandle = flogHandle

  logInit hnd = do
    (pos, sz) <- readLogPos hnd
    return $ FileLog hnd pos sz

  logAppend l rs = do
    let hnd = logHandle l
    let pos = flogPos l
    let pos' = pos + sum (map tRecSize rs)
    sz <- checkFileSize hnd (flogSize l) pos'
    liftIO . hSeek hnd AbsoluteSeek $ fromIntegral pos
    forM_ rs $ writeLogTRec hnd
    writeLogPos hnd $ fromIntegral pos'
    return l { flogPos = pos', flogSize = sz }

  logRead l = do
    let hnd = logHandle l
    pos <- liftIO $ hTell hnd
    if flogSize l == 0 || pos >= fromIntegral (flogPos l) - 1
    then return Nothing
    else do
      r <- readLogTRec hnd
      return $ Just r

tRecSize :: TRec -> Size
tRecSize r = fromIntegral $ case r of
  Pending dr  -> 16 + ws * (3 + 2 * (length (docURefs dr) +
                                     length (docIRefs dr) +
                                     length (docDRefs dr)))
  Completed _ -> 9
  where ws = sizeOf (0 :: IxKey)

readBits :: forall a m. (Storable a, MonadIO m) => Handle -> m a
readBits h = liftIO . alloca $ \ptr ->
  hGetBuf h ptr (sizeOf (undefined :: a)) >> peek ptr

writeBits :: (Storable a, MonadIO m) => Handle -> a -> m ()
writeBits h w = liftIO . with w $ \ptr ->
  hPutBuf h ptr (sizeOf w)

readLogPos :: MonadIO m => Handle -> m (Addr, Size)
readLogPos h = liftIO $ do
  sz <- hFileSize h
  if sz >= fromIntegral (sizeOf (0 :: Addr)) then do
    hSeek h AbsoluteSeek 0
    w <- readBits h
    return (w, fromIntegral sz)
  else
    return (fromIntegral $ sizeOf (0 :: Addr), 0)

writeLogPos :: MonadIO m => Handle -> Addr -> m ()
writeLogPos h p = liftIO $ do
  hSeek h AbsoluteSeek 0
  writeBits h p

checkFileSize :: MonadIO m => Handle -> Size -> Addr -> m Size
checkFileSize hnd osz pos =
  if pos > osz then do
    let sz = max pos $ osz + 4096
    liftIO . hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

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
      us <- readWordList h IntReference
      is <- readWordList h IntReference
      ds <- readWordList h DocReference
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

readWordList h con = do
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

logError :: MonadIO m => Handle -> ShowS -> m a
logError h err = liftIO $ do
  pos <- hTell h
  throw . LogParseError . showString "Log corrupted at position " . shows pos .
            showString ". " $ err ""
