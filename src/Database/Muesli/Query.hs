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
--
-- All queries in this module are run on indexes and perform an
-- __O(log n)__ worst case operation.
--
-- Functions whose name ends in \' do the same operation as their counterparts,
-- but return only the keys. As such, they work only on indexes and no I/O is
-- involved. They can be used to implement various kinds of joins not supported
-- by the primitive operations.
----------------------------------------------------------------------------

module Database.Muesli.Query
  ( module Database.Muesli.Types
  , module Database.Muesli.Backend.Types
-- * The Transaction monad
  , Transaction
  , runQuery
  , TransactionAbort (..)
-- * Primitive queries
-- ** CRUD operations
  , lookup
  , insert
  , update
  , delete
-- ** Range queries
  , range
  , range'
  , filterRange
  , filterRange'
  , filter
  , filter'
-- ** Queries on unique fields
  , unique
  , unique'
  , updateUnique
-- ** Other
  , size
  ) where

import           Control.Applicative           ((<|>))
import           Control.Exception             (throw)
import           Control.Monad                 (forM, liftM)
import qualified Control.Monad.State           as S
import           Control.Monad.Trans           (MonadIO)
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

-- | Dereferences the given key. Returns 'Nothing' if the key is not found.
lookup :: (Document a, LogState l, MonadIO m) => Reference a ->
           Transaction l m (Maybe a)
lookup (Reference did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc m t did
  mba <- maybe (return Nothing) (\(r, mbs) -> do
           a <- getDocument (transHandle t) r mbs
           return $ Just a)
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

-- | Returns a 'Reference' to a document uniquely determined by the given
-- 'Unique' key value, or 'Nothing' if the key is not found.
unique' :: (ToKey (Unique b), MonadIO m) =>
                 Property a -> Unique b -> Transaction l m (Maybe (Reference a))
unique' p ub = Transaction $ do
  t <- S.get
  let u = toKey ub
  withMasterLock (transHandle t) $ \m -> return . liftM Reference $
    findUnique pp u (transUpdateList t)
    <|> (findUnique pp u . concat . Map.elems $ logPend m)
    <|> liftM fromIntegral (IntMap.lookup (fromIntegral pp) (unqIdx m) >>=
                            IntMap.lookup (fromIntegral u))
  where pp = fst $ unProperty p


-- | Returns a document uniquely determined by the given
-- 'Unique' key value, or 'Nothing' if the key is not found.
unique :: (Document a, LogState l, ToKey (Unique b), MonadIO m) =>
                 Property a -> Unique b -> Transaction l m (Maybe (Reference a, a))
unique p ub =
  unique' p ub >>= maybe (return Nothing)
                         (\k -> liftM (liftM (k,)) (lookup k))

-- | Performs a 'unique'' and then, depending whether the key exists or not,
-- either 'insert's or 'update's the respective document.
updateUnique :: (Document a, ToKey (Unique b), MonadIO m) =>
                 Property a -> Unique b -> a -> Transaction l m (Reference a)
updateUnique p u a =
  unique' p u >>= maybe (insert a) (\did -> update did a >> return did)

-- | Updates a document.
--
-- Note that since @muesli@ is a MVCC database, this means inserting a new
-- version of the document. The version number is the 'TransactionId' of the
-- current transaction. This fact is transparent to the user though.
update :: forall a l m. (Document a, MonadIO m) =>
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

-- | Inserts a new document and returns its key.
--
-- The primary key is generated with 'mkNewDocumentKey'.
insert :: (Document a, MonadIO m) => a -> Transaction l m (Reference a)
insert a = Transaction $ do
  t <- S.get
  did <- mkNewDocumentKey $ transHandle t
  unTransaction $ update (Reference did) a
  return $ Reference did

-- | Deletes a document.
--
-- Note that since @muesli@ is a MVCC database, this means inserting a new
-- version with the 'recDeleted' flag set to 'True'.
-- But this fact is transparent to the user, since the indexes are updated
-- as if the record was really deleted.
--
-- It will be the job of the 'Database.Muesli.GC.gcThread' to actually
-- clean the transaction log and compact the data file.
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

-- | Runs a range query on a 'Sortable' field.
--
-- It can be used as a cursor, for precise and efficient paging through a
-- large dataset. For this purpose you should remember the last 'Reference'
-- from the previous page and give it as the /sortKey/ argument below.
-- This is needed since the sortable field may not have unique values, so
-- remembering just the /sortVal/ is insufficient.
--
-- The corresponding SQL is:
--
-- @
-- SELECT TOP page * FROM table
-- WHERE (sortVal = NULL OR sortFld < sortVal) AND (sortKey = NULL OR ID < sortKey)
-- ORDER BY sortFld, ID DESC
-- @
range :: (Document a, ToKey (Sortable b), LogState l, MonadIO m)
      => Int                 -- ^ The @page@ below.
      -> Property a          -- ^ The @sortFld@ and @table@ below.
      -> Maybe (Sortable b)  -- ^ The @sortVal@ in the below SQL.
      -> Maybe (Reference a) -- ^ The @sortKey@ below.
      -> Transaction l m [(Reference a, a)]
range pg p mst msti = page_ (range_ pg p msti) mst

range_ :: Document a => Int -> Property a -> Maybe (Reference b) -> Int ->
          MasterState l -> [Int]
range_ pg p msti st m = fromMaybe [] $ getPage st (rval msti) pg <$>
                                       IntMap.lookup (prop2Int p) (sortIdx m)

-- | Like 'range', but returns only the keys.
range' :: (Document a, ToKey (Sortable b), MonadIO m)
       => Int
       -> Property a
       -> Maybe (Sortable b)
       -> Maybe (Reference a)
       -> Transaction l m [Reference a]
range' pg p mst msti = pageK_ (range_ pg p msti) mst

-- | Runs a filter-and-range query on a 'Reference' field, with results sorted
-- on a different 'Sortable' field.
--
-- Sending 'Nothing' for @filterVal@ filters for @NULL@ values, which correspond
-- to a 'Nothing' in a field of type 'Maybe' ('Reference' a). This uses the
-- special 'Maybe' instance mentioned at the 'Indexable' documentation.
--
-- The paging behaviour is the same as for 'range'.
--
-- The corresponding SQL is:
--
-- @
-- SELECT TOP page * FROM table
-- WHERE (filterFld = filterVal) AND
--       (sortVal = NULL OR sortFld < sortVal) AND (sortKey = NULL OR ID < sortKey)
-- ORDER BY sortFld, ID DESC
-- @
filterRange :: (Document a, ToKey (Sortable b), LogState l, MonadIO m)
            => Int                  -- ^ The @page@ below.
            -> Property a           -- ^ The @sortFld@ and @table@ below.
            -> Maybe (Reference c)  -- ^ The @filterVal@ in the below SQL.
            -> Property a           -- ^ The @filterFld@ and @table@ below.
            -> Maybe (Sortable b)   -- ^ The @sortVal@ below.
            -> Maybe (Reference a)  -- ^ The @sortKey@ below.
            -> Transaction l m [(Reference a, a)]
filterRange pg fprop mdid sprop mst msti =
  page_ (filterRange_ pg fprop mdid sprop mst msti) mst

filterRange_ :: (ToKey (Sortable b), Document a) => Int -> Property a
             -> Maybe (Reference c) -> Property a -> Maybe (Sortable b)
             -> Maybe (Reference a) -> Int -> MasterState l -> [Int]
filterRange_ pg fprop mdid sprop mst msti _ m =
  fromMaybe [] . liftM (getPage (ival mst) (rval msti) pg) $
    IntMap.lookup (fromIntegral . fst . unProperty $ fprop) (refIdx m) >>=
    IntMap.lookup (fromIntegral $ maybe 0 unReference mdid) >>=
    IntMap.lookup (prop2Int sprop)

-- | Like 'filterRange', but returns only the keys.
filterRange' :: (Document a, ToKey (Sortable b), LogState l, MonadIO m)
             => Int
             -> Property a
             -> Maybe (Reference c)
             -> Property a
             -> Maybe (Sortable b)
             -> Maybe (Reference a)
             -> Transaction l m [Reference a]
filterRange' pg fprop mdid sprop mst msti =
  pageK_ (filterRange_ pg fprop mdid sprop mst msti) mst

-- | Runs a filter query on a 'Reference' field, with results sorted
-- on a different 'Sortable' field.
--
-- Unlike 'filterRange', it returnes all documents, not just a range.
filter :: (Document a, LogState l, MonadIO m)
       => Property a           -- ^ The @filterFld@ and @table@ below.
       -> Maybe (Reference b)  -- ^ The @filterVal@ in the below SQL.
       -> Property a           -- ^ The @sortFld@ and @table@ below.
       -> Transaction l m [(Reference a, a)]
filter fprop mdid sprop = filterRange maxBound fprop mdid sprop
                          (Nothing :: Maybe (Sortable Int)) Nothing

-- | Like 'filter', but returns only the keyes.
filter' :: (Document a, LogState l, MonadIO m)
        => Property a
        -> Maybe (Reference b)
        -> Property a
        -> Transaction l m [Reference a]
filter' fprop mdid sprop = filterRange' maxBound fprop mdid sprop
                           (Nothing :: Maybe (Sortable Int)) Nothing

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

pageK_ :: (MonadIO m, ToKey (Sortable b)) => (Int -> MasterState l -> [Int]) ->
           Maybe (Sortable b) -> Transaction l m [Reference a]
pageK_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> return $
    concatMap (map (recDocumentKey . fst) . foldMap pure . findFirstDoc m t .
    fromIntegral) $ f (ival mdid) m
  S.put t { transReadList = dds ++ transReadList t }
  return (Reference <$> dds)

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

-- | Returns the number of documents of type @a@ in the database.
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
