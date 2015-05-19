-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Handle
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides resource management functions.
----------------------------------------------------------------------------

module Database.Muesli.Handle
  ( Handle
  , open
  , close
  , performGC
  , DatabaseError (..)
  , debug
  ) where

import           Control.Concurrent          (forkIO, newMVar)
import           Control.Exception           (throw)
import           Control.Monad               (replicateM)
import           Control.Monad.Trans         (MonadIO (liftIO))
import qualified Data.ByteString             as B
import qualified Data.IntMap.Strict          as Map
import qualified Data.List                   as L
import           Data.Maybe                  (fromMaybe)
import           Data.Serialize              (decode)
import           Database.Muesli.Allocator
import qualified Database.Muesli.Cache       as Cache
import           Database.Muesli.Commit
import           Database.Muesli.GC
import           Database.Muesli.IdSupply    (emptyIdSupply)
import           Database.Muesli.Indexes
import           Database.Muesli.Internal
import           Database.Muesli.Transaction
import           Database.Muesli.Types
import           Database.Muesli.Utils
import           System.FilePath             ((</>))
import qualified System.IO                   as IO

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ IO.openBinaryFile logPath IO.ReadWriteMode
  dfh <- liftIO $ IO.openBinaryFile datPath IO.ReadWriteMode
  liftIO $ IO.hSetBuffering lfh IO.NoBuffering
  liftIO $ IO.hSetBuffering dfh IO.NoBuffering
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , logSize   = lsz
                      , idSupply  = emptyIdSupply
                      , keepTrans = False
                      , gaps      = emptyGaps 0
                      , logPend   = Map.empty
                      , logComp   = Map.empty
                      , mainIdx   = Map.empty
                      , unqIdx    = Map.empty
                      , intIdx    = Map.empty
                      , refIdx    = Map.empty
                      }
  m' <- if (pos > 0) && (fromIntegral lsz > dbWordSize)
        then readLog m 0
        else return m
  let m'' = m' { gaps = buildGaps $ mainIdx m' }
  mv <- liftIO $ newMVar m''
  let d = DataState { dataHandle = dfh
                    , dataCache  = Cache.empty 0x100000 (0x100000 * 10) 60
                    }
  dv <- liftIO $ newMVar d
  um <- liftIO $ newMVar False
  gc <- liftIO $ newMVar IdleGC
  let h = Handle DBState { logFilePath  = logPath
                         , dataFilePath = datPath
                         , masterState  = mv
                         , dataState    = dv
                         , updateMan    = um
                         , gcState      = gc
                         }
  liftIO . forkIO $ updateManThread h True
  liftIO . forkIO $ gcThread h
  return h

close :: MonadIO m => Handle -> m ()
close h = do
  withGC h . const $ return (KillGC, ())
  withUpdateMan h . const $ return (True, ())
  withMasterLock h $ \m -> IO.hClose (logHandle m)
  withDataLock   h $ \(DataState d _) -> IO.hClose d

performGC :: MonadIO m => Handle -> m ()
performGC h = withGC h . const $ return (PerformGC, ())

debug :: MonadIO m => Handle -> Bool -> Bool -> m String
debug h sIdx sCache = do
  mstr <- withMasterLock h $ \m -> return $
    showsH   "logPos    : " (logPos m) .
    showsH "\nlogSize   : " (logSize m) .
    showsH "\nidSupply  :\n  " (idSupply m) .
    showsH "\nlogPend   :\n  " (logPend m) .
    showsH "\nlogComp   :\n  " (logComp m) .
    if sIdx then
    showsH "\nmainIdx   :\n  " (mainIdx m) .
    showsH "\nunqIdx    :\n  " (unqIdx m) .
    showsH "\nintIdx    :\n  " (intIdx m) .
    showsH "\nrefIdx    :\n  " (refIdx m) .
    showsH "\ngaps      :\n  " (gaps m)
    else showString ""
  dstr <- withDataLock h $ \d -> return $
    showsH "\ncacheSize : " (Cache.cSize $ dataCache d) .
    if sCache then
    showsH "\ncache     :\n  " (Cache.cQueue $ dataCache d)
    else showString ""
  return $ mstr . dstr $ ""
  where showsH s a = showString s . shows a

readLogPos :: MonadIO m => IO.Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ IO.hFileSize h
  if sz >= fromIntegral dbWordSize then do
    liftIO $ IO.hSeek h IO.AbsoluteSeek 0
    w <- readWord h
    return (w, fromIntegral sz)
  else
    return (0, 0)

readLog :: MonadIO m => MasterState -> Int -> m MasterState
readLog m pos = do
  let h = logHandle m
  let l = logPend m
  ln <- readLogTRec h
  m' <- case ln of
          Pending r ->
            let tid = fromIntegral $ docTID r in
            let ids = reserveIdsRec (idSupply m) r in
            case Map.lookup tid l of
              Nothing -> return m { idSupply = ids
                                  , logPend  = Map.insert tid [(r, B.empty)] l }
              Just rs -> return m { idSupply = ids
                                  , logPend  = Map.insert tid ((r, B.empty):rs) l }
          Completed tid ->
            case Map.lookup (fromIntegral tid) l of
              Nothing -> logError h $ showString "Completed TID:" . shows tid .
                showString " found but transaction did not previously occur."
              Just rps -> let rs = fst <$> rps in
                          return m { logPend  = Map.delete (fromIntegral tid) l
                                   , mainIdx  = updateMainIdx (mainIdx m) rs
                                   , unqIdx   = updateUnqIdx (unqIdx m) rs
                                   , intIdx   = updateIntIdx (intIdx m) rs
                                   , refIdx   = updateRefIdx (refIdx m) rs
                                   }
  let pos' = pos + tRecSize ln
  if pos' >= (fromIntegral (logPos m) - 1)
  then return m' else readLog m' pos'

readWord :: MonadIO m => IO.Handle -> m DBWord
readWord h = do
  bs <- liftIO $ B.hGet h dbWordSize
  either (logError h . showString) return (decode bs)

readLogTRec :: MonadIO m => IO.Handle -> m TRec
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
               x | x == truTag  -> return True
               x | x == flsTag -> return False
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

logError :: MonadIO m => IO.Handle -> ShowS -> m a
logError h err = do
  pos <- liftIO $ IO.hTell h
  liftIO . throw . LogParseError (fromIntegral pos) . showString
    "Corrupted log. " $ err ""
