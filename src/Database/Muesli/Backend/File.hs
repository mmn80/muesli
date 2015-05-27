{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Backend.File
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Binary seekable file backend that uses 'Prelude' functions.
----------------------------------------------------------------------------

module Database.Muesli.Backend.File
  ( FileHandle
  , FileLogState (..)
  ) where

import           Control.Exception             (throw)
import           Control.Monad                 (forM_, replicateM, unless, when)
import           Control.Monad.Trans           (MonadIO (liftIO))
import           Data.ByteString               (hGet, hPut)
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

-- | @newtype@ wrapper around 'Handle', so that we can accomodate other instances.
newtype FileHandle = FileHandle { unIOHandle :: Handle }
  deriving (Eq, Show)

instance DbHandle FileHandle where
  openDb path = liftIO $ do
    hnd <- openBinaryFile path ReadWriteMode
    hSetBuffering hnd NoBuffering
    return $ FileHandle hnd

  closeDb hnd = liftIO . hClose $ unIOHandle hnd

  withDb path f = liftIO . withBinaryFile path ReadWriteMode $ f . FileHandle

  swapDb oldPath path = liftIO (renameFile path oldPath) >> openDb oldPath

instance DataHandle FileHandle where
  readDocument hnd r = do
    liftIO . hSeek (unIOHandle hnd) AbsoluteSeek . fromIntegral $ recAddress r
    liftIO . hGet (unIOHandle hnd) . fromIntegral $ recSize r

  writeDocument r bs hnd = unless (recDeleted r) . liftIO $ do
    let sz = fromIntegral $ recAddress r + recSize r
    let h = unIOHandle hnd
    osz <- hFileSize h
    when (osz < sz) $ do
      let nsz = max sz $ osz + 4096
      hSetFileSize h nsz
    hSeek h AbsoluteSeek . fromIntegral $ recAddress r
    hPut h bs

-- | Implements a stateful binary log file backend.
--
-- It uses a 'FileHandle' for both the log and data files.
data FileLogState = FileLogState
  {
-- | The 'Handle' for the log file
    flogHandle :: FileHandle
-- | Current valid position in the (quasi)append-only log file.
-- Located at address 0 of the log file, it is the last write performd as
-- part of a batch 'logAppend' operation, which makes it atomic.
  , flogPos    :: DocAddress
-- | Current size of the log file. 'logAppend' first checks the file size
-- and increases it with minimum 4KB if necessary, then writes the records,
-- and then updates the 'flogPos'.
  , flogSize   :: DocSize
  } deriving (Show)

instance LogState FileLogState where
  type LogHandleOf FileLogState = FileHandle
  type DataHandleOf FileLogState = FileHandle

  logHandle = flogHandle

  logInit hnd = do
    (pos, sz) <- readLogPos (unIOHandle hnd)
    return $ FileLogState hnd pos sz

  logAppend l rs = do
    let hnd = unIOHandle $ logHandle l
    let pos = flogPos l
    let pos' = pos + sum (map sizeTransRecord rs)
    sz <- checkFileSize hnd (flogSize l) pos'
    liftIO . hSeek hnd AbsoluteSeek $ fromIntegral pos
    forM_ rs $ writeTransRecord hnd
    writeLogPos hnd $ fromIntegral pos'
    return l { flogPos = pos', flogSize = sz }

  logRead l = do
    let hnd = unIOHandle $ logHandle l
    pos <- liftIO $ hTell hnd
    if flogSize l == 0 || pos >= fromIntegral (flogPos l) - 1
    then return Nothing
    else do
      r <- readTransRecord hnd
      return $ Just r

sizeTransRecord :: TransRecord -> DocSize
sizeTransRecord r = fromIntegral $ case r of
  Pending dr  -> 16 + ws * (3 + 2 * (length (recUniques dr) +
                                     length (recSortables dr) +
                                     length (recReferences dr)))
  Completed _ -> 9
  where ws = sizeOf (0 :: IxKey)

readBits :: forall a m. (Storable a, MonadIO m) => Handle -> m a
readBits h = liftIO . alloca $ \ptr ->
  hGetBuf h ptr (sizeOf (undefined :: a)) >> peek ptr

writeBits :: (Storable a, MonadIO m) => Handle -> a -> m ()
writeBits h w = liftIO . with w $ \ptr ->
  hPutBuf h ptr (sizeOf w)

readLogPos :: MonadIO m => Handle -> m (DocAddress, DocSize)
readLogPos h = liftIO $ do
  sz <- hFileSize h
  if sz >= fromIntegral (sizeOf (0 :: DocAddress)) then do
    hSeek h AbsoluteSeek 0
    w <- readBits h
    return (w, fromIntegral sz)
  else
    return (fromIntegral $ sizeOf (0 :: DocAddress), 0)

writeLogPos :: MonadIO m => Handle -> DocAddress -> m ()
writeLogPos h p = liftIO $ do
  hSeek h AbsoluteSeek 0
  writeBits h p

checkFileSize :: MonadIO m => Handle -> DocSize -> DocAddress -> m DocSize
checkFileSize hnd osz pos =
  if pos > osz then do
    let sz = max pos $ osz + 4096
    liftIO . hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

readTransRecord :: MonadIO m => Handle -> m TransRecord
readTransRecord h = do
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
      us <- readWordList h
      is <- readWordList h
      ds <- readWordList h
      return $ Pending LogRecord { recDocumentKey   = did
                                 , recTransactionId = tid
                                 , recUniques       = us
                                 , recSortables     = is
                                 , recReferences    = ds
                                 , recAddress       = adr
                                 , recSize          = siz
                                 , recDeleted       = dlb
                                 }
    x | x == cmpTag -> do
      tid <- readBits h
      return $ Completed tid
    _ -> logError h $ showString "Pending ('p') or Completed ('c') tag expected but " .
           shows tag . showString " found."

readWordList :: MonadIO m => Handle -> m [(PropertyKey, IxKey)]
readWordList h = do
  sz <- readBits h
  replicateM (fromIntegral (sz :: Word16)) $ do
    pid <- readBits h
    val <- readBits h
    return (pid, val)

writeTransRecord :: MonadIO m => Handle -> TransRecord -> m ()
writeTransRecord h t =
  case t of
    Pending doc -> do
      writeBits h pndTag
      writeBits h $ recTransactionId doc
      writeBits h $ recDocumentKey doc
      writeBits h $ recAddress doc
      writeBits h $ recSize doc
      writeBits h $ if recDeleted doc then truTag else flsTag
      writeWordList h $ recUniques doc
      writeWordList h $ recSortables doc
      writeWordList h $ recReferences doc
    Completed tid -> do
      writeBits h cmpTag
      writeBits h tid

writeWordList :: MonadIO m => Handle -> [(PropertyKey, IxKey)] -> m ()
writeWordList h rs = do
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
