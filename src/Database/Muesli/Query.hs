{-# LANGUAGE ScopedTypeVariables #-}

-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Query
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the Transaction monad and its primitive queries.
----------------------------------------------------------------------------

module Database.Muesli.Query
  ( module Database.Muesli.Types
  , Transaction
  , TransactionAbort (..)
  , runQuery
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
  ) where

import           Control.Exception      (throw)
import           Control.Monad          (forM, liftM, mplus)
import qualified Control.Monad.State    as S
import           Control.Monad.Trans    (MonadIO (liftIO))
import qualified Data.ByteString        as B
import           Data.IntMap.Strict     (IntMap)
import qualified Data.IntMap.Strict     as Map
import           Data.IntSet            (IntSet)
import qualified Data.IntSet            as Set
import qualified Data.List              as L
import           Data.Maybe             (fromMaybe)
import           Data.Serialize         (Serialize (..), decode, encode)
import           Data.String            (IsString (..))
import           Data.Time.Clock        (getCurrentTime)
import           Data.Typeable          (Typeable)
import qualified Database.Muesli.Cache  as Cache
import           Database.Muesli.Commit
import           Database.Muesli.IO
import           Database.Muesli.State
import           Database.Muesli.Types
import           Prelude                hiding (filter, lookup)

lookup :: (Document a, MonadIO m) => DocID a -> Transaction m (Maybe (DocID a, a))
lookup (DocID did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc m t did
  mba <- maybe (return Nothing) (\r -> do
           a <- getDocument (transHandle t) r
           return $ Just (DocID did, a))
         mbr
  S.put t { transReadList = did : transReadList t }
  return mba

findFirstDoc :: MasterState -> TransactionState -> DID -> Maybe DocRecord
findFirstDoc m t did = do
  r <- Map.lookup (fromIntegral did) (mainIdx m) >>= L.find ((<= transTID t) . docTID)
  if docDel r then Nothing else Just r

lookupUnique :: MonadIO m => Property a -> IntVal b -> Transaction m (Maybe (DocID a))
lookupUnique p (IntVal u) = Transaction $ do
  t <- S.get
  withMasterLock (transHandle t) $ \m -> return . liftM DocID $
    findUnique pp u (transUpdateList t)
    `mplus` (findUnique pp u . concat . Map.elems $ logPend m)
    `mplus` liftM fromIntegral (Map.lookup (fromIntegral pp) (unqIdx m) >>=
                                Map.lookup (fromIntegral u))
  where pp = fst $ unProperty p

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
    fd (p, rid) = DocReference (getP p) rid
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
    a <- getDocument (transHandle t) d
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
                  Map.lookup (fromIntegral . fst . unProperty $ fprop) (refIdx m) >>=
                  Map.lookup (fromIntegral $ maybe 0 unDocID mdid) >>=
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
getPage sta sti pg idx = go sta pg []
  where go st p acc =
          if p == 0 then acc
          else case Map.lookupLT st idx of
                 Nothing      -> acc
                 Just (n, is) ->
                   let (p', ids) = getPage2 sti p is in
                   if p' == 0 then ids ++ acc
                   else go n p' $ ids ++ acc

getPage2 :: Int -> Int -> IntSet -> (Int, [Int])
getPage2 sta pg idx = go sta pg []
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

rval :: Maybe (DocID a) -> Int
rval = fromIntegral . unDocID . fromMaybe maxBound

ival :: Maybe (IntVal a) -> Int
ival = fromIntegral . unIntVal. fromMaybe maxBound

prop2Int :: Document a => Property a -> Int
prop2Int = fromIntegral . fst . unProperty

getDocument :: (Typeable a, Serialize a, MonadIO m) => Handle -> DocRecord -> m a
getDocument h r =
  withData h $ \(DataState hnd cache) -> do
    now <- getCurrentTime
    let k = fromIntegral $ docAddr r
    case Cache.lookup now k cache of
      Nothing -> do
        bs <- readDocument hnd r
        a <- either (liftIO . throw . DataParseError k (fromIntegral $ docSize r) .
                    showString "Deserialization error: ")
             return (decode bs)
        return (DataState hnd $ Cache.insert now k a (B.length bs) cache, a)
      Just (a, _, cache') -> return (DataState hnd cache', a)
