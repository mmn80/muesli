{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Query
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- The 'Transaction' monad and its primitive queries.
----------------------------------------------------------------------------

module Database.Muesli.Query
  ( module Database.Muesli.Types
  , module Database.Muesli.Backend.Types
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

import           Control.Applicative           ((<|>))
import           Control.Exception             (throw)
import           Control.Monad                 (forM, liftM)
import qualified Control.Monad.State           as S
import           Control.Monad.Trans           (MonadIO (liftIO))
import           Data.ByteString               (ByteString)
import qualified Data.ByteString               as B
import           Data.IntMap.Strict            (IntMap)
import qualified Data.IntMap.Strict            as IntMap
import           Data.IntSet                   (IntSet)
import qualified Data.IntSet                   as Set
import qualified Data.List                     as L
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromMaybe)
import           Data.Serialize                (Serialize (..), decode, encode)
import           Data.String                   (IsString (..))
import           Data.Time.Clock               (getCurrentTime)
import           Data.Typeable                 (Typeable)
import           Database.Muesli.Backend.Types
import qualified Database.Muesli.Cache         as Cache
import           Database.Muesli.Commit
import           Database.Muesli.State
import           Database.Muesli.Types
import           Prelude                       hiding (filter, lookup)

lookup :: (Document a, LogState l, MonadIO m) => Reference a ->
           Transaction l m (Maybe (Reference a, a))
lookup (Reference did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc m t did
  mba <- maybe (return Nothing) (\(r, mbs) -> do
           a <- getDocument (transHandle t) r mbs
           return $ Just (Reference did, a))
         mbr
  S.put t { transReadList = did : transReadList t }
  return mba

findFirstDoc :: MasterState l -> TransactionState l -> DocumentKey ->
                Maybe (LogRecord, Maybe ByteString)
findFirstDoc m t did = do
  (r, mbs) <- liftM (fmap Just)
                    (L.find ((== did) . recDocumentKey . fst) (transUpdateList t))
          <|> liftM (, Nothing)
                    (IntMap.lookup (fromIntegral did) (mainIdx m) >>=
                     L.find ((<= transId t) . recTransactionId))
  if recDeleted r then Nothing else Just (r, mbs)

lookupUnique :: (ToKey (Unique b), MonadIO m) =>
                 Property a -> Unique b -> Transaction l m (Maybe (Reference a))
lookupUnique p ub = Transaction $ do
  t <- S.get
  let u = toKey ub
  withMasterLock (transHandle t) $ \m -> return . liftM Reference $
    findUnique pp u (transUpdateList t)
    <|> (findUnique pp u . concat . Map.elems $ logPend m)
    <|> liftM fromIntegral (IntMap.lookup (fromIntegral pp) (unqIdx m) >>=
                            IntMap.lookup (fromIntegral u))
  where pp = fst $ unProperty p

updateUnique :: (Document a, ToKey (Unique b), MonadIO m) =>
                 Property a -> Unique b -> a -> Transaction l m (Reference a)
updateUnique p u a = do
  mdid <- lookupUnique p u
  case mdid of
    Nothing  -> insert a
    Just did -> update did a >> return did

update :: forall a l d m. (Document a, MonadIO m) =>
          Reference a -> a -> Transaction l m ()
update did a = Transaction $ do
  t <- S.get
  let bs = encode a
  let is = getIndexables a
  let r = LogRecord { recDocumentKey   = unReference did
                    , recTransactionId = transId t
                    , recUniques       = p2k <$> ixUniques is
                    , recSortables     = p2k <$> ixSortables is
                    , recReferences    = p2k <$> ixReferences is
                    , recAddress       = 0
                    , recSize          = fromIntegral $ B.length bs
                    , recDeleted       = False
                    }
  S.put t { transUpdateList = (r, bs) : transUpdateList t }
  where
    p2k (p, val) = (getP p, val)
    getP p = fst $ unProperty (fromString p :: Property a)

insert :: (Document a, MonadIO m) => a -> Transaction l m (Reference a)
insert a = Transaction $ do
  t <- S.get
  did <- mkNewDocumentKey $ transHandle t
  unTransaction $ update (Reference did) a
  return $ Reference did

delete :: MonadIO m => Reference a -> Transaction l m ()
delete (Reference did) = Transaction $ do
  t <- S.get
  mb <- withMasterLock (transHandle t) $ \m -> return $ findFirstDoc m t did
  let r = LogRecord { recDocumentKey   = did
                    , recTransactionId = transId t
                    , recUniques       = maybe [] (recUniques . fst) mb
                    , recSortables     = maybe [] (recSortables . fst) mb
                    , recReferences    = maybe [] (recReferences . fst) mb
                    , recAddress       = maybe 0  (recAddress  . fst) mb
                    , recSize          = maybe 0  (recSize  . fst) mb
                    , recDeleted       = True
                    }
  S.put t { transUpdateList = (r, B.empty) : transUpdateList t }

page_ :: (Document a, ToKey (Sortable b), LogState l, MonadIO m) =>
         (Int -> MasterState l -> [Int]) -> Maybe (Sortable b) ->
          Transaction l m [(Reference a, a)]
page_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> do
           let ds = f (ival mdid) m
           let mbds = findFirstDoc m t . fromIntegral <$> ds
           return $ concatMap (foldMap pure) mbds
  dds' <- forM (reverse dds) $ \(d, mbs) -> do
    a <- getDocument (transHandle t) d mbs
    return (Reference $ recDocumentKey d, a)
  S.put t { transReadList = (unReference . fst <$> dds') ++ transReadList t }
  return dds'

range :: (Document a, ToKey (Sortable b), LogState l, MonadIO m) =>
          Maybe (Sortable b) -> Maybe (Reference a) -> Property a -> Int ->
          Transaction l m [(Reference a, a)]
range mst msti p pg = page_ f mst
  where f st m = fromMaybe [] $ do
                   ds <- IntMap.lookup (prop2Int p) (sortIdx m)
                   return $ getPage st (rval msti) pg ds

filter :: (Document a, ToKey (Sortable b), LogState l, MonadIO m) =>
           Maybe (Reference c) -> Maybe (Sortable b) -> Maybe (Reference a) ->
           Property a -> Property a -> Int -> Transaction l m [(Reference a, a)]
filter mdid mst msti fprop sprop pg = page_ f mst
  where f _ m = fromMaybe [] . liftM (getPage (ival mst) (rval msti) pg) $
                  IntMap.lookup (fromIntegral . fst . unProperty $ fprop) (refIdx m) >>=
                  IntMap.lookup (fromIntegral $ maybe 0 unReference mdid) >>=
                  IntMap.lookup (prop2Int sprop)

pageK_ :: (MonadIO m, ToKey (Sortable b)) => (Int -> MasterState l -> [Int]) ->
           Maybe (Sortable b) -> Transaction l m [Reference a]
pageK_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> return $
    concatMap (map (recDocumentKey . fst) . foldMap pure . findFirstDoc m t .
    fromIntegral) $ f (ival mdid) m
  S.put t { transReadList = dds ++ transReadList t }
  return (Reference <$> dds)

rangeK :: (Document a, ToKey (Sortable b), MonadIO m) => Maybe (Sortable b) ->
           Maybe (Reference a) -> Property a -> Int -> Transaction l m [Reference a]
rangeK mst msti p pg = pageK_ f mst
  where f st m = fromMaybe [] $ getPage st (rval msti) pg <$>
                                IntMap.lookup (prop2Int p) (sortIdx m)

getPage :: Int -> Int -> Int -> IntMap IntSet -> [Int]
getPage sta sti pg idx = go sta pg []
  where go st p acc =
          if p == 0 then acc
          else case IntMap.lookupLT st idx of
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

size :: (Document a, MonadIO m) => Property a -> Transaction l m Int
size p = Transaction $ do
  t <- S.get
  withMasterLock (transHandle t) $ \m -> return . fromMaybe 0 $
    (sum . map (Set.size . snd) . IntMap.toList) <$>
     IntMap.lookup (prop2Int p) (sortIdx m)

rval :: Maybe (Reference a) -> Int
rval = fromIntegral . unReference . fromMaybe maxBound

ival :: ToKey (Sortable a) => Maybe (Sortable a) -> Int
ival = fromIntegral . maybe maxBound toKey

prop2Int :: Document a => Property a -> Int
prop2Int = fromIntegral . fst . unProperty

getDocument :: (Typeable a, Serialize a, LogState l, MonadIO m) =>
                Handle l -> LogRecord -> Maybe ByteString -> m a
getDocument h r mbs =
  withData h $ \(DataState hnd cache) -> do
    now <- getCurrentTime
    let k = fromIntegral $ recAddress r
    let decodeBs bs =
          either (throw . DataParseError (recAddress r) (recSize r) .
                  showString "Deserialization error: ")
          (\a -> return (DataState hnd $ Cache.insert now k a (B.length bs) cache, a))
          (decode bs)
    case mbs of
      Just bs -> decodeBs bs
      Nothing -> case Cache.lookup now k cache of
                   Just (a, _, cache') -> return (DataState hnd cache', a)
                   Nothing -> readDocument hnd r >>= decodeBs
