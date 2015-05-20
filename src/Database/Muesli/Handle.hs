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

import           Control.Concurrent        (forkIO, newMVar)
import           Control.Monad.Trans       (MonadIO (liftIO))
import qualified Data.ByteString           as B
import qualified Data.IntMap.Strict        as Map
import           Data.Maybe                (fromMaybe)
import qualified Database.Muesli.Allocator as Gaps
import qualified Database.Muesli.Cache     as Cache
import           Database.Muesli.Commit
import           Database.Muesli.GC
import qualified Database.Muesli.IdSupply  as Ids
import           Database.Muesli.Indexes
import           Database.Muesli.IO
import           Database.Muesli.State
import           Database.Muesli.Types
import           System.FilePath           ((</>))
import           System.IO                 (BufferMode (..), IOMode (..),
                                            hClose, hSetBuffering,
                                            openBinaryFile)

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ openBinaryFile logPath ReadWriteMode
  dfh <- liftIO $ openBinaryFile datPath ReadWriteMode
  liftIO $ hSetBuffering lfh NoBuffering
  liftIO $ hSetBuffering dfh NoBuffering
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , logSize   = lsz
                      , idSupply  = Ids.empty
                      , keepTrans = False
                      , gaps      = Gaps.empty 0
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
  let m'' = m' { gaps = Gaps.build $ mainIdx m' }
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

close :: MonadIO m => Handle -> m ()
close h = do
  withGC h . const $ return (KillGC, ())
  withUpdateMan h . const $ return (True, ())
  withMasterLock h $ \m -> hClose (logHandle m)
  withDataLock   h $ \(DataState d _) -> hClose d
