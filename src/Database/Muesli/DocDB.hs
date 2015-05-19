{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeOperators              #-}

-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.DocDB
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the database.
----------------------------------------------------------------------------

module Database.Muesli.DocDB
  ( Handle
  , open
  , close
  , performGC
  , Transaction
  , runTransaction
  , TransactionAbort (..)
  , DatabaseError (..)
  , Property
  , DocID
  , IntVal
  , intVal
  , intValUnique
  , Unique (..)
  , Indexable (..)
  , DBValue
  , Document (..)
  , lookup
  , insert
  , update
  , delete
  , range
  , rangeK
  , filter
  , lookupUnique
  , updateUnique
  , size
  , debug
  ) where

import           Control.Arrow         ((&&&))
import           Control.Concurrent    (MVar, ThreadId, forkIO, newMVar,
                                        putMVar, takeMVar, threadDelay)
import           Control.Exception     (Exception, bracket, bracketOnError,
                                        throw)
import           Control.Monad         (forM, forM_, liftM, mplus, replicateM,
                                        unless, when)
import           Control.Monad.State   (StateT)
import qualified Control.Monad.State   as S
import           Control.Monad.Trans   (MonadIO (liftIO))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import           Data.Function         (on)
import           Data.Hashable         (Hashable, hash)
import           Data.IntMap.Strict    (IntMap, (\\))
import qualified Data.IntMap.Strict    as Map
import           Data.IntSet           (IntSet)
import qualified Data.IntSet           as Set
import qualified Data.List             as L
import           Data.Maybe            (fromJust, fromMaybe)
import           Data.Serialize        (Serialize (..), decode, encode)
import           Data.Serialize.Get    (getWord32be)
import           Data.Serialize.Put    (putWord32be)
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word32, Word8)
import           Database.Muesli.Cache (LRUCache)
import qualified Database.Muesli.Cache as Cache
import           GHC.Generics          ((:*:) (..), (:+:) (..), C, D, Generic,
                                        K1 (..), M1 (..), Rep, S, Selector,
                                        U1 (..), from, selName)
import           GHC.TypeLits          (Symbol)
import           Numeric               (showHex)
import           Prelude               hiding (filter, lookup)
import           System.Directory      (renameFile)
import           System.FilePath       ((</>))
import qualified System.IO             as IO

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral)

instance Serialize DBWord where
  put = putWord32be . unDBWord
  get = DBWord <$> getWord32be

instance Show DBWord where
  showsPrec p = showsPrec p . unDBWord

toInt :: DBWord -> Int
toInt (DBWord d) = fromIntegral d

type Addr     = DBWord
type TID      = DBWord
type DID      = DBWord
type IntValue = DBWord
type Size     = DBWord
type PropID   = DBWord

data DocReference = DocReference
  { drefPID :: !PropID
  , drefDID :: !DID
  } deriving (Show)

data IntReference = IntReference
  { irefPID :: !PropID
  , irefVal :: !IntValue
  } deriving (Show)

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

type SortIndex = IntMap (IntMap IntSet)

type FilterIndex = IntMap (IntMap SortIndex)

type UniqueIndex = IntMap (IntMap Int)

type IdSupply = IntMap Size

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: !Addr
  , logSize   :: !Size
  , idSupply  :: !IdSupply
  , keepTrans :: !Bool
  , gaps      :: !(IntMap [Addr])
  , logPend   :: !(IntMap [(DocRecord, ByteString)])
  , logComp   :: !(IntMap [DocRecord])
  , mainIdx   :: !(IntMap [DocRecord])
  , unqIdx    :: !UniqueIndex
  , intIdx    :: !SortIndex
  , refIdx    :: !FilterIndex
  }

data DataState = DataState
  { dataHandle :: IO.Handle
  , dataCache  :: !LRUCache
  }

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , masterState  :: MVar MasterState
  , dataState    :: MVar DataState
  , updateMan    :: MVar Bool
  , gcState      :: MVar GCState
  }

instance Eq DBState where
  s == s' = logFilePath s == logFilePath s'

data TRec = Pending DocRecord | Completed TID
  deriving (Show)

isPending (Pending _)   = True
isPending (Completed _) = False
fromPending (Pending r) = r

-- External Types --------------------------------------------------------------

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  showsPrec p = showsPrec p . logFilePath . unHandle

data TransactionState = TransactionState
  { transHandle     :: Handle
  , transTID        :: !TID
  , transReadList   :: ![DID]
  , transUpdateList :: ![(DocRecord, ByteString)]
  }

newtype Transaction m a = Transaction { unTransaction :: StateT TransactionState m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction m) where
  liftIO = Transaction . liftIO

data TransactionAbort = AbortUnique String
                      | AbortConflict String
                      | AbortDelete String
  deriving (Show)

instance Exception TransactionAbort

data DatabaseError = LogParseError Int String
                   | DataParseError Int Int String
                   | IdAllocationError String
                   | DataAllocationError Int (Maybe Int) String
  deriving (Show)

instance Exception DatabaseError

-- Properties ------------------------------------------------------------------

newtype Property a = Property { unProperty :: (PropID, String) }

instance Eq (Property a) where
  Property (pid, _) == Property (pid', _) = pid == pid'

instance Show (Property a) where
  showsPrec p (Property (pid, s)) = showString s . showString "[" .
    showsPrec p pid . showString "]"

instance Typeable a => IsString (Property a) where
  fromString s = Property (pid, s)
    where pid = fromIntegral $ hash (show $ typeRep (Proxy :: Proxy a), s)

prop2Int :: Document a => Property a -> Int
prop2Int = toInt . fst . unProperty

-- Values ----------------------------------------------------------------------

newtype DocID a = DocID { unDocID :: DID }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral, Serialize)

instance Show (DocID a) where
  showsPrec _ (DocID k) = showString "0x" . showHex k

rval :: Maybe (DocID a) -> Int
rval = toInt . unDocID . fromMaybe maxBound

newtype IntVal a = IntVal { unIntVal :: DID }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral)

instance Show (IntVal a) where
  showsPrec _ (IntVal k) = showString "0x" . showHex k

ival :: Maybe (IntVal a) -> Int
ival = toInt . unIntVal. fromMaybe maxBound

intVal :: DBValue (Indexable a) => a -> IntVal b
intVal = fromIntegral . head . getDBValues . Indexable

intValUnique :: DBValue (Unique a) => a -> IntVal b
intValUnique = fromIntegral . fromJust . getUnique . Unique

newtype Indexable a = Indexable { unIndexable :: a }
  deriving (Eq, Ord, Bounded, Serialize)

instance Show a => Show (Indexable a) where
  showsPrec p (Indexable a) = showsPrec p a

newtype Unique a = Unique { unUnique :: a }
  deriving (Eq, Serialize)

instance Show a => Show (Unique a) where
  showsPrec p (Unique a) = showsPrec p a

class DBValue a where
  getDBValues :: a -> [DBWord]
  getDBValues _ = []

  isReference :: Proxy a -> Bool
  isReference _ = False

  getUnique :: a -> Maybe IntValue
  getUnique _ = Nothing

instance DBValue (DocID a) where
  getDBValues (DocID did) = [ did ]
  isReference _ = True

instance DBValue (Maybe (DocID a)) where
  getDBValues mb = [ maybe 0 unDocID mb ]
  isReference _ = True

instance {-# OVERLAPPABLE #-} (DBValue a, Foldable f) => DBValue (f a) where
  getDBValues = foldMap getDBValues
  isReference _ = isReference (Proxy :: Proxy a)

instance DBValue Bool
instance DBValue Int
instance DBValue String

instance DBValue (Indexable UTCTime) where
  getDBValues (Indexable t) = [ round $ utcTimeToPOSIXSeconds t ]
  isReference _ = False

instance DBValue (Indexable DBWord) where
  getDBValues (Indexable w) = [ w ]
  isReference _ = False

instance DBValue (Indexable Int) where
  getDBValues (Indexable a) = [ fromIntegral a ]
  isReference _ = False

instance {-# OVERLAPPABLE #-} Show a => DBValue (Indexable a) where
  getDBValues (Indexable a) = [ snd $ L.foldl' f (wordSize - 1, 0) bytes ]
    where bytes = (fromIntegral . fromEnum <$> take wordSize (show a)) :: [Word8]
          f (n, v) b = (n - 1, if n >= 0 then v + fromIntegral b * 2 ^ (8 * n) else v)
  isReference _ = False

instance (Hashable a, DBValue (Indexable a)) => DBValue (Unique (Indexable a)) where
  getUnique (Unique (Indexable a)) = Just . fromIntegral $ hash a
  getDBValues = getDBValues . unUnique
  isReference _ = isReference (Proxy :: Proxy (Indexable a))

instance {-# OVERLAPPABLE #-} Hashable a => DBValue (Unique a) where
  getUnique (Unique a) = Just . fromIntegral $ hash a

-- Records ---------------------------------------------------------------------

data Indexables = Indexables
  { ixRefs :: [(String, DID)]
  , ixInts :: [(String, DID)]
  , ixUnqs :: [(String, IntValue)]
  } deriving (Show)

class (Typeable a, Generic a, Serialize a) => Document a where
  getIndexables :: a -> Indexables
  default getIndexables :: (GetIndexables (Rep a)) => a -> Indexables
  getIndexables = ggetIndexables "" . from

class GetIndexables f where
  ggetIndexables :: String -> f a -> Indexables

instance GetIndexables U1 where
  ggetIndexables _ U1 = Indexables [] [] []

instance (GetIndexables a, GetIndexables b) => GetIndexables (a :*: b) where
  ggetIndexables _ (x :*: y) = Indexables (xrs ++ yrs) (xis ++ yis) (xus ++ yus)
    where (Indexables xrs xis xus) = ggetIndexables "" x
          (Indexables yrs yis yus) = ggetIndexables "" y

instance (GetIndexables a, GetIndexables b) => GetIndexables (a :+: b) where
  ggetIndexables _ (L1 x) = ggetIndexables "" x
  ggetIndexables _ (R1 x) = ggetIndexables "" x

instance GetIndexables a => GetIndexables (M1 D c a) where
  ggetIndexables _ m1@(M1 x) = ggetIndexables "" x

instance GetIndexables a => GetIndexables (M1 C c a) where
  ggetIndexables _ m1@(M1 x) = ggetIndexables "" x

instance (GetIndexables a, Selector c) => GetIndexables (M1 S c a) where
  ggetIndexables _ m1@(M1 x) = ggetIndexables (selName m1) x

instance DBValue a => GetIndexables (K1 i a) where
  ggetIndexables n (K1 x) =
    if isReference (Proxy :: Proxy a)
    then Indexables { ixRefs = vs, ixInts = [], ixUnqs = us }
    else Indexables { ixRefs = [], ixInts = vs, ixUnqs = us }
    where vs = (\did -> (n, did)) <$> getDBValues x
          us = maybe [] (pure . (n,)) (getUnique x)

-- Resource Management -------------------------------------------------------

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
  m' <- if (pos > 0) && (toInt lsz > wordSize)
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

runTransaction :: MonadIO m => Handle -> Transaction m a ->
                  m (Either TransactionAbort a)
runTransaction h (Transaction t) = do
  tid <- mkNewId h
  (a, q, u) <- runUserCode tid
  if null u then return $ Right a
  else withMaster h $ \m ->
    if | not $ checkUnique u (unqIdx m) (logPend m) -> return (m, Left $
           AbortUnique "Transaction aborted: uniqueness check failed.")
       | not $ checkConflict tid (logPend m) (logComp m) q u -> return (m, Left $
           AbortConflict "Transaction aborted: conflict with concurrent transactions.")
       | not $ checkDelete m u -> return (m, Left $
           AbortDelete "Document cannot be deleted. Other documents still point to it.")
       | otherwise -> do
           let tsz = sum $ (tRecSize . Pending . fst) <$> u
           let pos = toInt (logPos m) + tsz
           lsz <- checkLogSize (logHandle m) (toInt (logSize m)) pos
           let (ts, gs) = L.foldl' allocFold ([], gaps m) u
           let m' = m { logPos  = fromIntegral pos
                      , logSize = fromIntegral lsz
                      , gaps    = gs
                      , logPend = Map.insert (toInt tid) ts $ logPend m
                      }
           writeTransactions m ts
           writeLogPos (logHandle m) $ fromIntegral pos
           return (m', Right a)
  where
    runUserCode tid = do
      (a, TransactionState _ _ q u) <- S.runStateT t
        TransactionState
          { transHandle     = h
          , transTID        = tid
          , transReadList   = []
          , transUpdateList = []
          }
      let u' = L.nubBy ((==) `on` docID . fst) u
      return (a, q, u')

    checkUnique u idx logp = all ck u
      where ck (d, _) = docDel d || all (cku $ docID d) (docURefs d)
            cku did (IntReference pid val) =
              cku' did (liftM fromIntegral $ Map.lookup (toInt pid) idx >>=
                                             Map.lookup (toInt val)) &&
              cku' did (findUnique pid val (concat $ Map.elems logp))
            cku' did = maybe True (== did)

    checkConflict tid logp logc q u = ck qs && ck us
      where
        us = (\(d,_) -> (toInt (docID d), ())) <$> u
        qs = (\k -> (toInt k, ())) <$> q
        ck lst = Map.null (Map.intersection newPs ml) &&
                 Map.null (Map.intersection newCs ml)
          where ml = Map.fromList lst
        newPs = snd $ Map.split (toInt tid) logp
        newCs = snd $ Map.split (toInt tid) logc

    checkDelete m u = all ck . L.filter docDel $ map fst u
      where ck d = all (ckEmpty $ toInt did) lst && all (ckPnd did) rs
              where did = docID d
            lst = Map.elems $ refIdx m
            ckEmpty did idx =
              case Map.lookup did idx of
                Nothing -> True
                Just ss -> all null $ Map.elems ss
            rs = L.filter (not . docDel) . map fst . concat . Map.elems $ logPend m
            ckPnd did r = not . any ((did ==) . drefDID) $ docDRefs r

    allocFold (ts, gs) (r, bs) =
      if docDel r then ((r, bs):ts, gs)
      else ((t, bs):ts, gs')
        where (a, gs') = alloc gs $ docSize r
              t = r { docAddr = a }

    writeTransactions m ts = do
      logSeek m
      forM_ ts $ \(t, _) ->
        writeLogTRec (logHandle m) $ Pending t

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

-- Queries --------------------------------------------------------------------

lookup :: (Document a, MonadIO m) => DocID a -> Transaction m (Maybe (DocID a, a))
lookup (DocID did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc m t did
  mba <- maybe (return Nothing) (\r -> do
           a <- readDocument (transHandle t) r
           return $ Just (DocID did, a))
         mbr
  S.put t { transReadList = did : transReadList t }
  return mba

findFirstDoc :: MasterState -> TransactionState -> DID -> Maybe DocRecord
findFirstDoc m t did = do
  r <- Map.lookup (toInt did) (mainIdx m) >>= L.find ((<= transTID t) . docTID)
  if docDel r then Nothing else Just r

lookupUnique :: MonadIO m => Property a -> IntVal b -> Transaction m (Maybe (DocID a))
lookupUnique p (IntVal u) = Transaction $ do
  t <- S.get
  withMasterLock (transHandle t) $ \m -> return . liftM DocID $
    findUnique pp u (transUpdateList t)
    `mplus` (findUnique pp u . concat . Map.elems $ logPend m)
    `mplus` liftM fromIntegral (Map.lookup (toInt pp) (unqIdx m) >>=
                                Map.lookup (toInt u))
  where pp = fst $ unProperty p

findUnique :: IntValue -> IntValue -> [(DocRecord, a)] -> Maybe DID
findUnique p u rs = fmap docID . L.find findR $ map fst rs
  where findR = any (\i -> irefPID i == p && irefVal i == u) . docURefs

updateUnique :: (Document a, MonadIO m) => Property a -> IntVal b -> a ->
                Transaction m (DocID a)
updateUnique p u a = do
  mdid <- lookupUnique p u
  case mdid of
    Nothing  -> insert a
    Just did -> update did a >> return did

update :: forall a m. (Document a, MonadIO m) => DocID a -> a -> Transaction m ()
update did a = Transaction $ do
  t <- S.get
  let bs = encode a
  let is = getIndexables a
  let r = DocRecord { docID   = unDocID did
                    , docTID  = transTID t
                    , docURefs = fi <$> ixUnqs is
                    , docIRefs = fi <$> ixInts is
                    , docDRefs = fd <$> ixRefs is
                    , docAddr = 0
                    , docSize = fromIntegral $ B.length bs
                    , docDel  = False
                    }
  S.put t { transUpdateList = (r, bs) : transUpdateList t }
  where
    fi (p, val) = IntReference (getP p) val
    fd (p, did) = DocReference (getP p) did
    getP p = fst $ unProperty (fromString p :: Property a)

insert :: (Document a, MonadIO m) => a -> Transaction m (DocID a)
insert a = Transaction $ do
  t <- S.get
  tid <- mkNewId $ transHandle t
  unTransaction $ update (DocID tid) a
  return $ DocID tid

delete :: MonadIO m => DocID a -> Transaction m ()
delete (DocID did) = Transaction $ do
  t <- S.get
  mb <- withMasterLock (transHandle t) $ \m -> return $ findFirstDoc m t did
  let r = DocRecord { docID    = did
                    , docTID   = transTID t
                    , docURefs = maybe [] docURefs mb
                    , docIRefs = maybe [] docIRefs mb
                    , docDRefs = maybe [] docDRefs mb
                    , docAddr  = maybe 0 docAddr mb
                    , docSize  = maybe 0 docSize mb
                    , docDel   = True
                    }
  S.put t { transUpdateList = (r, B.empty) : transUpdateList t }

page_ :: (Document a, MonadIO m) => (Int -> MasterState -> [Int]) ->
         Maybe (IntVal b) -> Transaction m [(DocID a, a)]
page_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> do
           let ds = f (ival mdid) m
           let mbds = findFirstDoc m t . fromIntegral <$> ds
           return $ concatMap (foldMap pure) mbds
  dds' <- forM (reverse dds) $ \d -> do
    a <- readDocument (transHandle t) d
    return (DocID $ docID d, a)
  S.put t { transReadList = (unDocID . fst <$> dds') ++ transReadList t }
  return dds'

range :: (Document a, MonadIO m) => Maybe (IntVal b) -> Maybe (DocID a) ->
         Property a -> Int -> Transaction m [(DocID a, a)]
range mst msti p pg = page_ f mst
  where f st m = fromMaybe [] $ do
                   ds <- Map.lookup (prop2Int p) (intIdx m)
                   return $ getPage st (rval msti) pg ds

filter :: (Document a, MonadIO m) => Maybe (DocID c) -> Maybe (IntVal b) ->
          Maybe (DocID a) -> Property a -> Property a -> Int ->
          Transaction m [(DocID a, a)]
filter mdid mst msti fprop sprop pg = page_ f mst
  where f _ m = fromMaybe [] . liftM (getPage (ival mst) (rval msti) pg) $
                  Map.lookup (toInt . fst . unProperty $ fprop) (refIdx m) >>=
                  Map.lookup (toInt $ maybe 0 unDocID mdid) >>=
                  Map.lookup (prop2Int sprop)

pageK_ :: MonadIO m => (Int -> MasterState -> [Int]) -> Maybe (IntVal b) ->
          Transaction m [DocID a]
pageK_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> return $
    concatMap (map docID . foldMap pure . findFirstDoc m t . fromIntegral) $
    f (ival mdid) m
  S.put t { transReadList = dds ++ transReadList t }
  return (DocID <$> dds)

rangeK :: (Document a, MonadIO m) => Maybe (IntVal b) -> Maybe (DocID a) ->
          Property a -> Int -> Transaction m [DocID a]
rangeK mst msti p pg = pageK_ f mst
  where f st m = fromMaybe [] $ getPage st (rval msti) pg <$>
                                Map.lookup (prop2Int p) (intIdx m)

getPage :: Int -> Int -> Int -> IntMap IntSet -> [Int]
getPage st sti p idx = go st p []
  where go st p acc =
          if p == 0 then acc
          else case Map.lookupLT st idx of
                 Nothing      -> acc
                 Just (n, is) ->
                   let (p', ids) = getPage2 sti p is in
                   if p' == 0 then ids ++ acc
                   else go n p' $ ids ++ acc

getPage2 :: Int -> Int -> IntSet -> (Int, [Int])
getPage2 st p idx = go st p []
  where go st p acc =
          if p == 0 then (0, acc)
          else case Set.lookupLT st idx of
                 Nothing -> (p, acc)
                 Just a  -> go a (p - 1) (a:acc)

size :: (Document a, MonadIO m) => Property a -> Transaction m Int
size p = Transaction $ do
  t <- S.get
  withMasterLock (transHandle t) $ \m -> return . fromMaybe 0 $
    (sum . map (Set.size . snd) . Map.toList) <$> Map.lookup (prop2Int p) (intIdx m)

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

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

wordSize :: Int
wordSize = 4

readWord :: MonadIO m => IO.Handle -> m DBWord
readWord h = do
  bs <- liftIO $ B.hGet h wordSize
  either (logError h . showString) return (decode bs)

writeWord :: MonadIO m => IO.Handle -> DBWord -> m ()
writeWord h w = liftIO . B.hPut h $ encode w

readLogPos :: MonadIO m => IO.Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ IO.hFileSize h
  if sz >= fromIntegral wordSize then do
    liftIO $ IO.hSeek h IO.AbsoluteSeek 0
    w <- readWord h
    return (w, fromIntegral sz)
  else
    return (0, 0)

writeLogPos :: MonadIO m => IO.Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ IO.hSeek h IO.AbsoluteSeek 0
  liftIO $ B.hPut h $ encode p

checkLogSize :: MonadIO m => IO.Handle -> Int -> Int -> m Int
checkLogSize hnd osz pos =
  let bpos = wordSize * (pos + 1) in
  if bpos > osz then do
    let sz = max bpos $ osz + 4096
    liftIO . IO.hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

logSeek :: MonadIO m => MasterState -> m ()
logSeek m = liftIO $ IO.hSeek h IO.AbsoluteSeek p
  where h = logHandle m
        p = fromIntegral $ wordSize * toInt (1 + logPos m)

readLog :: MonadIO m => MasterState -> Int -> m MasterState
readLog m pos = do
  let h = logHandle m
  let l = logPend m
  ln <- readLogTRec h
  m' <- case ln of
          Pending r ->
            let tid = toInt $ docTID r in
            let ids = reserveIdsRec (idSupply m) r in
            case Map.lookup tid l of
              Nothing -> return m { idSupply = ids
                                  , logPend  = Map.insert tid [(r, B.empty)] l }
              Just rs -> return m { idSupply = ids
                                  , logPend  = Map.insert tid ((r, B.empty):rs) l }
          Completed tid ->
            case Map.lookup (toInt tid) l of
              Nothing -> logError h $ showString "Completed TID:" . shows tid .
                showString " found but transaction did not previously occur."
              Just rps -> let rs = fst <$> rps in
                          return m { logPend  = Map.delete (toInt tid) l
                                   , mainIdx  = updateMainIdx (mainIdx m) rs
                                   , unqIdx   = updateUnqIdx (unqIdx m) rs
                                   , intIdx   = updateIntIdx (intIdx m) rs
                                   , refIdx   = updateRefIdx (refIdx m) rs
                                   }
  let pos' = pos + tRecSize ln
  if pos' >= (fromIntegral (logPos m) - 1)
  then return m' else readLog m' pos'

pndTag :: Int
pndTag = 0x70 -- ASCII 'p'

cmpTag :: Int
cmpTag = 0x63 -- ASCII 'c'

truTag :: Int
truTag = 0x54 -- ASCII 'T'

flsTag :: Int
flsTag = 0x46 -- ASCII 'F'

readLogTRec :: MonadIO m => IO.Handle -> m TRec
readLogTRec h = do
  tag <- readWord h
  case toInt tag of
    x | x == pndTag -> do
      tid <- readWord h
      did <- readWord h
      adr <- readWord h
      siz <- readWord h
      del <- readWord h
      dlb <- case toInt del of
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
          replicateM (toInt sz) $ do
            pid <- readWord h
            val <- readWord h
            return $ con pid val


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

readDocument :: (Typeable a, Serialize a, MonadIO m) => Handle -> DocRecord -> m a
readDocument h r =
  withData h $ \(DataState hnd cache) -> do
    now <- getCurrentTime
    let k = toInt $ docAddr r
    case Cache.lookup now k cache of
      Nothing -> do
        bs <- readDocumentFromFile hnd r
        a <- either (liftIO . throw . DataParseError k (toInt $ docSize r) .
                    showString "Deserialization error: ")
             return (decode bs)
        return (DataState hnd $ Cache.insert now k a (B.length bs) cache, a)
      Just (a, _, cache') -> return (DataState hnd cache', a)

readDocumentFromFile :: MonadIO m => IO.Handle -> DocRecord -> m ByteString
readDocumentFromFile hnd r = do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO . B.hGet hnd . fromIntegral $ docSize r

writeDocument :: MonadIO m => DocRecord -> ByteString -> IO.Handle -> m ()
writeDocument r bs hnd = unless (docDel r) $ do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO $ B.hPut hnd bs

logError :: MonadIO m => IO.Handle -> ShowS -> m a
logError h err = do
  pos <- liftIO $ IO.hTell h
  liftIO . throw . LogParseError (fromIntegral pos) . showString
    "Corrupted log. " $ err ""

-- Indexes -------------------------------------------------------------------

updateMainIdx :: IntMap [DocRecord] -> [DocRecord] -> IntMap [DocRecord]
updateMainIdx = L.foldl' f
  where f idx r = let did = toInt (docID r) in
                  let rs' = maybe [r] (r:) (Map.lookup did idx) in
                  Map.insert did rs' idx

updateUnqIdx :: UniqueIndex -> [DocRecord] -> UniqueIndex
updateUnqIdx = L.foldl' f
  where f idx r = L.foldl' g idx (docURefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
              let rpid = toInt (irefPID ref) in
              let rval = toInt (irefVal ref) in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval did) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = if del then Map.delete rval is
                              else Map.insert rval did is

updateIntIdx :: SortIndex -> [DocRecord] -> SortIndex
updateIntIdx = L.foldl' f
  where f idx r = L.foldl' g idx (docIRefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
              let rpid = toInt (irefPID ref) in
              let rval = toInt (irefVal ref) in
              let sng = Set.singleton did in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> if Set.null ss' then Map.delete rval is
                                           else Map.insert rval ss' is
                                  where ss' = if del then Set.delete did ss
                                              else Set.insert did ss

updateRefIdx :: FilterIndex -> [DocRecord] -> FilterIndex
updateRefIdx = L.foldl' f
  where f idx r = L.foldl' g idx (docDRefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
              let rpid = toInt (drefPID ref) in
              let rval = toInt (drefDID ref) in
              let sng = updateIntIdx Map.empty [r] in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> Map.insert rval ss' is
                                  where ss' = updateIntIdx ss [r]

-- ID Allocator --------------------------------------------------------------

emptyIdSupply :: IdSupply
emptyIdSupply = Map.singleton 1 (maxBound - 1)

reserveIdsRec :: IdSupply -> DocRecord -> IdSupply
reserveIdsRec s r = reserveId (docTID r) $ reserveId (docID r) s

reserveId :: TID -> IdSupply -> IdSupply
reserveId tidb s = maybe s
  (\(st, szdb) ->
      let sz = toInt szdb in
      let delta = tidb - fromIntegral st in
      if | tid >= st + sz       -> s
         | tid == st && sz == 1 -> Map.delete st s
         | tid == st            -> Map.insert (tid + 1) (szdb - 1) $
                                   Map.delete st s
         | tid == st + sz - 1   -> Map.insert st (szdb - 1) s
         | otherwise            -> Map.insert (tid + 1) (szdb - delta - 1) $
                                   Map.insert st delta s)
  (Map.lookupLE tid s)
  where tid = toInt tidb

allocId :: IdSupply -> (TID, IdSupply)
allocId s =
  case Map.lookupGE 0 s of
    Nothing -> throw $ IdAllocationError "ID allocation error: supply empty."
    Just (st, sz) ->
      let tid = fromIntegral st in
      if sz == 1 then (tid, Map.delete st s)
      else (tid, Map.insert (st + 1) (sz - 1) $ Map.delete st s)

mkNewId :: MonadIO m => Handle -> m TID
mkNewId h = withMaster h $ \m ->
  let (tid, s) = allocId (idSupply m) in
  return (m { idSupply = s }, tid)

-- Data Allocator ------------------------------------------------------------

emptyGaps :: Addr -> IntMap [Addr]
emptyGaps sz = Map.singleton (toInt $ maxBound - sz) [sz]

addGap :: Size -> Addr -> IntMap [Addr] -> IntMap [Addr]
addGap s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = toInt s

buildGaps :: IntMap [DocRecord] -> IntMap [Addr]
buildGaps idx = addTail . L.foldl' f (Map.empty, 0) . L.sortOn docAddr .
               L.filter (not . docDel) . map head $ Map.elems idx
  where
    f (gs, addr) r = (gs', docAddr r + docSize r)
      where gs' = if addr == docAddr r then gs
                  else addGap sz addr gs
            sz = docAddr r - addr
    addTail (gs, addr) = addGap (maxBound - addr) addr gs

alloc :: IntMap [Addr] -> Size -> (Addr, IntMap [Addr])
alloc gs s = let sz = toInt s in
  case Map.lookupGE sz gs of
    Nothing -> throw $ DataAllocationError sz (fst <$> Map.lookupLT maxBound gs)
                       "Data allocation error."
    Just (gsz, a:as) ->
      if delta == 0 then (a, gs')
      else (a, addGap (fromIntegral delta) (a + fromIntegral sz) gs')
      where gs' = if null as then Map.delete gsz gs
                             else Map.insert gsz as gs
            delta = gsz - sz

-- Update Manager ------------------------------------------------------------

updateManThread :: Handle -> Bool -> IO ()
updateManThread h w = do
  (kill, wait) <- withUpdateMan h $ \kill -> do
    wait <- if kill then return True else do
      when w . threadDelay $ 100 * 1000
      withMasterLock h $ \m ->
        let lgp = logPend m in
        if null lgp then return Nothing
        else return . Just $ Map.findMin lgp
      >>=
      maybe (return True) (\(tid, rs) -> do
        withData h $ \(DataState hnd cache) -> do
          let maxAddr = toInteger . maximum $ (\r -> docAddr r + docSize r) .
                        fst <$> rs
          sz <- IO.hFileSize hnd
          when (sz < maxAddr + 1) $ do
            let nsz = max (maxAddr + 1) $ sz + 4096
            IO.hSetFileSize hnd nsz
          forM_ rs $ \(r, bs) -> writeDocument r bs hnd
          let cache' = L.foldl' (\c (r, _) -> Cache.delete (toInt $ docAddr r) c)
                         cache rs
          return (DataState hnd cache', ())
        withMaster h $ \m -> do
          let rs' = fst <$> rs
          let trec = Completed $ fromIntegral tid
          let pos = fromIntegral (logPos m) + tRecSize trec
          lsz <- checkLogSize (logHandle m) (toInt (logSize m)) pos
          logSeek m
          let lh = logHandle m
          writeLogTRec lh trec
          writeLogPos lh $ fromIntegral pos
          let (lgp, lgc) = updateLog tid (keepTrans m) (logPend m) (logComp m)
          let m' = m { logPos  = fromIntegral pos
                     , logSize = fromIntegral lsz
                     , logPend = lgp
                     , logComp = lgc
                     , mainIdx = updateMainIdx (mainIdx m) rs'
                     , unqIdx  = updateUnqIdx (unqIdx m) rs'
                     , intIdx  = updateIntIdx (intIdx m) rs'
                     , refIdx  = updateRefIdx (refIdx m) rs'
                     }
          return (m', null lgp))
    return (kill, (kill, wait))
  unless kill $ updateManThread h wait
  where updateLog tid keep lgp lgc = (lgp', lgc')
          where ors  = map fst . fromMaybe [] $ Map.lookup tid lgp
                lgp' = Map.delete tid lgp
                lc   = if not keep && null lgp'
                       then Map.empty else lgc
                lgc' = if keep || not (null lgp')
                       then Map.insert tid ors lc else lc

-- Garbage Collector ---------------------------------------------------------

gcThread :: Handle -> IO ()
gcThread h = do
  sgn <- withGC h $ \sgn -> do
    when (sgn == PerformGC) $ do
      (mainIdxOld, logCompOld) <- withMaster h $ \m ->
        return (m { keepTrans = True }, (mainIdx m, logComp m))
      let rs  = map head . L.filter (not . any docDel) $ Map.elems mainIdxOld
      let (rs2, dpos) = realloc 0 rs
      let rs' = L.sortOn docTID $ map fst rs2
      let ts  = concatMap toTRecs $ L.groupBy ((==) `on` docTID) rs'
      let ids = L.foldl' reserveIdsRec emptyIdSupply . map fromPending $
                L.filter isPending ts
      let pos = sum $ tRecSize <$> ts
      let logPath = logFilePath (unHandle h)
      let logPathNew = logPath ++ ".new"
      sz <- IO.withBinaryFile logPathNew IO.ReadWriteMode $ writeTrans 0 pos ts
      let dataPath = dataFilePath (unHandle h)
      let dataPathNew = dataPath ++ ".new"
      IO.withBinaryFile dataPathNew IO.ReadWriteMode $ writeData rs2 dpos h
      let mIdx = updateMainIdx Map.empty rs'
      let uIdx = updateUnqIdx  Map.empty rs'
      let iIdx = updateIntIdx  Map.empty rs'
      let rIdx = updateRefIdx  Map.empty rs'
      when (forceEval mIdx iIdx rIdx) $ withUpdateMan h $ \kill -> do
        withMaster h $ \nm -> do
          let (ncrs', dpos') = realloc dpos . concat . Map.elems $
                               logComp nm \\ logCompOld
          let (logp', dpos'') = realloc' dpos' $ logPend nm
          let ncrs = fst <$> ncrs'
          (pos', sz') <-
            if null ncrs then return (pos, sz)
            else IO.withBinaryFile logPathNew IO.ReadWriteMode $ \hnd -> do
                   let newts = toTRecs ncrs
                   let pos' = pos + sum (tRecSize <$> newts)
                   sz' <- writeTrans sz pos' newts hnd
                   return (pos', sz')
          IO.hClose $ logHandle nm
          renameFile logPathNew logPath
          hnd <- IO.openBinaryFile logPath IO.ReadWriteMode
          IO.hSetBuffering hnd IO.NoBuffering
          IO.withBinaryFile dataPathNew IO.ReadWriteMode $ writeData ncrs' dpos'' h
          let gs = buildExtraGaps dpos'' . L.filter docDel $
                     ncrs ++ (map fst . concat $ Map.elems logp')
          let m = MasterState { logHandle = hnd
                              , logPos    = fromIntegral pos'
                              , logSize   = fromIntegral sz'
                              , idSupply  = ids
                              , keepTrans = False
                              , gaps      = gs
                              , logPend   = logp'
                              , logComp   = Map.empty
                              , mainIdx   = updateMainIdx mIdx ncrs
                              , unqIdx    = updateUnqIdx  uIdx ncrs
                              , intIdx    = updateIntIdx  iIdx ncrs
                              , refIdx    = updateRefIdx  rIdx ncrs
                              }
          return (m, ())
        withData h $ \(DataState hnd cache) -> do
          IO.hClose hnd
          renameFile dataPathNew dataPath
          hnd' <- IO.openBinaryFile dataPath IO.ReadWriteMode
          IO.hSetBuffering hnd' IO.NoBuffering
          return (DataState hnd' cache, ())
        return (kill, ())
    let sgn' = if sgn == PerformGC then IdleGC else sgn
    return (sgn', sgn')
  unless (sgn == KillGC) $ do
    threadDelay $ 1000 * 1000
    gcThread h
  where
    toTRecs rs = L.foldl' (\ts r -> Pending r : ts)
                 [Completed . docTID $ head rs] rs

    writeTrans osz pos ts hnd = do
      sz <- checkLogSize hnd osz pos
      IO.hSeek hnd IO.AbsoluteSeek $ fromIntegral wordSize
      forM_ ts $ writeLogTRec hnd
      writeLogPos hnd $ fromIntegral pos
      return sz

    writeData rs sz h hnd = do
      IO.hSetFileSize hnd $ fromIntegral sz
      forM_ rs $ \(r, oldr) -> do
        bs <- withDataLock h $ \(DataState hnd _) -> readDocumentFromFile hnd oldr
        writeDocument r bs hnd

    realloc st = L.foldl' f ([], st)
      where f (nrs, pos) r =
              if docDel r then ((r, r) : nrs, pos)
              else ((r { docAddr = pos }, r) : nrs, pos + docSize r)

    realloc' st idx = (Map.fromList l, pos)
      where (l, pos) = L.foldl' f ([], st) $ Map.toList idx
            f (lst, pos) (tid, rs) = ((tid, rs') : lst, pos')
              where (rss', pos') = realloc pos $ fst <$> rs
                    rs' = (fst <$> rss') `zip` (snd <$> rs)

    buildExtraGaps pos = L.foldl' f (emptyGaps pos)
      where f gs r = addGap (docSize r) (docAddr r) gs

    forceEval mIdx iIdx rIdx = Map.notMember (-1) mIdx &&
                               Map.size iIdx > (-1) &&
                               Map.size rIdx > (-1)
