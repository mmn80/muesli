-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Handle
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Resource management types and functions.
----------------------------------------------------------------------------

module Database.Muesli.Handle
  ( module Database.Muesli.Types
  , module Database.Muesli.Backend.Types
  , Handle
  , open
  , close
  , performGC
  , DatabaseError (..)
  , debug
  ) where

import           Control.Concurrent            (forkIO, newMVar)
import           Control.Exception             (throw)
import           Control.Monad.Trans           (MonadIO (liftIO))
import qualified Data.ByteString               as B
import qualified Data.IntMap.Strict            as IntMap
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromMaybe)
import           Data.Time                     (UTCTime)
import qualified Database.Muesli.Allocator     as GapsIndex
import           Database.Muesli.Backend.Types
import qualified Database.Muesli.Cache         as Cache
import           Database.Muesli.Commit
import           Database.Muesli.GC
import qualified Database.Muesli.IdSupply      as Ids
import           Database.Muesli.Indexes
import           Database.Muesli.State
import           Database.Muesli.Types
import           Foreign                       (sizeOf)
import           System.FilePath               ((</>))

open :: (MonadIO m, LogState l, DataHandle d) =>
         Maybe DbPath -> Maybe DbPath -> m (Handle l d)
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  dh <- openDb datPath
  st <- openDb logPath >>= logInit
  let m = MasterState { logState  = st
                      , topTid    = 0
                      , idSupply  = Ids.empty
                      , keepTrans = False
                      , gaps      = GapsIndex.empty 0
                      , logPend   = Map.empty
                      , logComp   = Map.empty
                      , mainIdx   = IntMap.empty
                      , unqIdx    = IntMap.empty
                      , sortIdx    = IntMap.empty
                      , refIdx    = IntMap.empty
                      }
  m' <- readLog m
  let m'' = m' { gaps = GapsIndex.build $ mainIdx m' }
  mv <- liftIO $ newMVar m''
  let d = DataState { dataHandle = dh
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

readLog :: (MonadIO m, LogState l) => MasterState l -> m (MasterState l)
readLog m = do
  let logp  = logPend m
  mbln <- logRead (logState m)
  case mbln of
    Nothing -> return m
    Just ln -> readLog $ case ln of
      Pending r ->
        let tid = recTransactionId r in
        let ids = Ids.reserve (recDocumentKey r) (idSupply m) in
        case Map.lookup tid logp of
          Nothing -> m { topTid   = max tid (topTid m)
                       , idSupply = ids
                       , logPend  = Map.insert tid [(r, B.empty)] logp }
          Just rs -> m { topTid   = max tid (topTid m)
                       , idSupply = ids
                       , logPend  = Map.insert tid ((r, B.empty):rs) logp }
      Completed tid ->
        case Map.lookup tid logp of
          Nothing -> throw . LogParseError . showString "Completed TransactionId:" $
                       shows tid " found for nonexisting transaction."
          Just rps -> let rs = fst <$> rps in
                      m { logPend  = Map.delete tid logp
                        , mainIdx  = updateMainIdx (mainIdx m) rs
                        , unqIdx   = updateUnqIdx (unqIdx m) rs
                        , sortIdx  = updateSortIdx (sortIdx m) rs
                        , refIdx   = updateRefIdx (refIdx m) rs
                        }

performGC :: MonadIO m => Handle l d -> m ()
performGC h = withGC h . const $ return (PerformGC, ())

debug :: (MonadIO m, LogState l) => Handle l d -> Bool -> Bool -> m String
debug h sIdx sCache = do
  mstr <- withMasterLock h $ \m -> return $
    showsH   "logState  : "    (logState m) .
    showsH "\ntopTid    : "    (topTid m) .
    showsH "\nidSupply  :\n  " (idSupply m) .
    showsH "\nlogPend   :\n  " (logPend m) .
    showsH "\nlogComp   :\n  " (logComp m) .
    if sIdx then
    showsH "\nmainIdx   :\n  " (mainIdx m) .
    showsH "\nunqIdx    :\n  " (unqIdx m) .
    showsH "\nsortIdx   :\n  " (sortIdx m) .
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

close :: (MonadIO m, LogState l, DbHandle d) => Handle l d -> m ()
close h = do
  withGC h . const $ return (KillGC, ())
  withUpdateMan h . const $ return (True, ())
  withMasterLock h $ \m -> closeDb $ logHandle (logState m)
  withDataLock   h $ \(DataState d _) -> closeDb d
