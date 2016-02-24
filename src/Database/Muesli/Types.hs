{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Types
-- Copyright   : (c) 2015-16 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Muesli markup types and typeclasses.
--
-- Normally, with the @DeriveAnyClass@ and @DeriveGeneric@ extensions enabled,
-- types can be marked up as documents with a
--
-- @
-- deriving (Generic, Serialize)
-- @
--
-- clause, and a separate empty 'Document' instance.
--
-- For structured field types, the following will suffice:
--
-- @
-- deriving (Generic, Serialize, Indexable)
-- @
--
-- Then 'Reference', 'Sortable' and 'Unique' can be used inside document types
-- to mark up the fields that should be automatically indexed, becoming queryable
-- with the primitives in "Database.Muesli.Query".
--
-- The record syntax must be used, and the accesor name will become the
-- 'Property' name used in queries.
----------------------------------------------------------------------------

module Database.Muesli.Types
  (
-- * Indexable value wrappers
    Reference (..)
  , Sortable (..)
  , Unique (..)
-- * Indexable value classes
  , Indexable (..)
  , Document (..)
  , Indexables (..)
-- * Index keyes
  , IxKey (..)
  , ToKey (..)
  , DocumentKey
  , SortableKey
  , UniqueKey
  , PropertyKey
-- * Other
  , Property (..)
  , DateTime (..)
  , DatabaseError (..)
  , TransactionId
  , DocAddress
  , DocSize
  ) where

import           Control.Exception     (Exception)
import           Control.Monad         (liftM)
import           Data.Bits             (Bits, FiniteBits)
import           Data.Data             (Data)
import           Data.Hashable         (Hashable, hash)
import           Data.List             (foldl')
import           Data.Serialize        (Serialize (..))
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           Data.Time.Format      (FormatTime, ParseTime)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word64, Word8)
import           Foreign               (Storable, sizeOf)
import           GHC.Generics          ((:*:) (..), (:+:) (..), C, D, Generic,
                                        K1 (..), M1 (..), Rep, S, Selector,
                                        U1 (..), from, selName)
import           Numeric               (showHex)

-- | Transaction ids are auto-incremented globally.
-- See 'Database.Muesli.State.mkNewTransactionId'.
type TransactionId = Word64
-- | Address in the abstract data file of a serialized document's data.
--
-- These addresses are allocated by functions in the module
-- "Database.Muesli.Allocator".
type DocAddress    = Word64
-- | Size of a serialized document's data.
type DocSize       = Word64

-- | A @newtype@ wrapper around 'UTCTime' that has 'Show', 'Serialize' and
-- 'ToKey' instances.
newtype DateTime = DateTime { unDateTime :: UTCTime }
  deriving (Eq, Ord, Data, ParseTime, FormatTime)

instance Show DateTime where
  showsPrec p = showsPrec p . unDateTime

instance Serialize DateTime where
  put = put . toRational . utcTimeToPOSIXSeconds . unDateTime
  get = liftM (DateTime . posixSecondsToUTCTime . fromRational) get

-- | Type for exceptions thrown by the database.
-- During normal operation these should never be thrown.
data DatabaseError
  -- | Thrown when the log file is corrupted.
  = LogParseError String
  -- | Thrown after deserialization errors.
  -- Holds starting position, size, and a message.
  | DataParseError DocAddress DocSize String
  -- | ID allocation failure. For instance, full address space on a 32 bit machine.
  | IdAllocationError String
  -- | Data allocation failure.
  -- Containes the size requested, the biggest available gap, and a message.
  | DataAllocationError DocSize (Maybe DocSize) String
  deriving (Show)

instance Exception DatabaseError

-- | Used inside indexes and as arguments to query primitives.
newtype IxKey = IxKey { unIxKey :: Int }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral,
            Bits, FiniteBits, Storable, Serialize)

instance Show IxKey where
  showsPrec _ (IxKey k) = showString "0x" . showHex k

-- | Class used to convert indexable field & query argument values to keys.
--
-- It is possibly needed for users to write instances for 'IxKey'-convertible
-- primitive types, other then the provided 'Bool', 'Int' and 'DateTime'.
class ToKey a where
  toKey :: a -> IxKey

-- | Primary key for a document.
--
-- Allocated by functions in the "Database.Muesli.IdSupply" module.
type DocumentKey = IxKey
-- | Key extracted from non-reference fields for building a
-- 'Database.Muesli.State.SortIndex'.
type SortableKey = IxKey
-- | Key extracted from unique fields for building a
-- 'Database.Muesli.State.UniqueIndex'
type UniqueKey   = IxKey
-- | Key generated by 'Property' for building a
-- 'Database.Muesli.State.SortIndex' or a 'Database.Muesli.State.FilterIndex'.
type PropertyKey = IxKey

-- | With the @OverloadedStrings@ extension, or directly using the 'IsString'
-- instance, field names specified in query arguments are converted to
-- 'Property'. An 'IxKey' is computed by hashing the property name together
-- with the 'Data.Typeable.TypeRep' of the phantom argument.
-- This key is used in indexes.
newtype Property a = Property { unProperty :: (PropertyKey, String) }

instance Eq (Property a) where
  Property (pid, _) == Property (pid', _) = pid == pid'

instance Show (Property a) where
  showsPrec p (Property (pid, s)) = showString s . showString "[" .
    showsPrec p pid . showString "]"

instance Typeable a => IsString (Property a) where
  fromString s = Property (pid, s)
    where pid = fromIntegral $ hash (show $ typeRep (Proxy :: Proxy a), s)

-- | 'Reference' fields are pointers to other 'Document's. They are
-- indexed automatically and can be queried with 'Database.Muesli.Query.filter'.
--
-- To ensure type safety, use and store only references returned by the
-- primitive queries, like 'Database.Muesli.Query.insert',
-- 'Database.Muesli.Query.range' or 'Database.Muesli.Query.filter'.
-- Numerical instances like 'Num' or 'Integral' are provided to support
-- generic database programs that take the responsibility of maintaining
-- invariants upon themselves. In this context, type safety means that it is
-- impossible in normal operation to try and deserialize a document at a wrong
-- type or address (note that all primitive query functions are polymorphic),
-- and risk getting bogus data without errors being reported.
newtype Reference a = Reference { unReference :: IxKey }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral, Serialize)

instance Show (Reference a) where
  showsPrec p = showsPrec p . unReference

-- | Marks a field available for sorting and 'range' queries.
--
-- Indexing requires a 'ToKey' instance. Apart from the provided 'Bool', 'Int'
-- and 'DateTime' instances, there is an overlappable fallback instance based on
-- converting the 'Show' string representation to an 'IxKey' by taking the first
-- 4 or 8 bytes. This is good enough for primitive string sorting.
newtype Sortable a = Sortable { unSortable :: a }
  deriving (Eq, Ord, Bounded, Serialize)

instance Show a => Show (Sortable a) where
  showsPrec p = showsPrec p . unSortable

instance ToKey (Sortable IxKey) where
  toKey (Sortable w) = w

instance ToKey (Sortable Bool) where
  toKey (Sortable b) = if b then 1 else 0

instance ToKey (Sortable Int) where
  toKey (Sortable a) = fromIntegral a

instance ToKey (Sortable DateTime) where
  toKey (Sortable (DateTime t)) = round $ utcTimeToPOSIXSeconds t

instance {-# OVERLAPPABLE #-} Show a => ToKey (Sortable a) where
  toKey (Sortable a) = snd $ foldl' f (ws - 1, 0) bytes
    where bytes = (fromIntegral . fromEnum <$> take ws str) :: [Word8]
          f (n, v) b = (n - 1, if n >= 0 then v + fromIntegral b * 2 ^ (8 * n) else v)
          ws = sizeOf (0 :: IxKey)
          str = case show a of
                  '"':as -> as
                  ss     -> ss

-- | 'Unique' fields act as primary keys, and can be queried with
-- 'Database.Muesli.Query.unique' and 'Database.Muesli.Query.updateUnique'.
-- The 'Hashable' instance is used to generate the key.
--
-- For fields that need to be both unique and sortable, use
-- 'Unique' ('Sortable' a) rather then the other way around.
newtype Unique a = Unique { unUnique :: a }
  deriving (Eq, Serialize)

instance Show a => Show (Unique a) where
  showsPrec p = showsPrec p . unUnique

instance Hashable a => ToKey (Unique (Sortable a)) where
  toKey (Unique (Sortable a)) = fromIntegral $ hash a

instance {-# OVERLAPPABLE #-} Hashable a => ToKey (Unique a) where
  toKey (Unique a) = fromIntegral $ hash a

-- | This class is used by the generic scrapper to extract indexable keys from
-- the fields of a 'Document'. There are instances for 'Reference', 'Sortable'
-- and 'Unique', a general 'Foldable' instance, and a special one for 'Maybe'
-- that converts 'Nothing' into 0, such that null values will be indexed too,
-- and become queryable with 'Database.Muesli.Query.filter'.
--
-- Users are not expected to need writing instances for this class.
-- They can rather be generated automatically with the @DeriveAnyClass@
-- extension.
class Indexable a where
  getIxValues :: a -> [IxKey]
  getIxValues _ = []

  isReference :: Proxy a -> Bool
  isReference _ = False

  getUnique :: a -> Maybe UniqueKey
  getUnique _ = Nothing

instance Indexable (Reference a) where
  getIxValues (Reference did) = [ did ]
  isReference _ = True

instance Indexable (Maybe (Reference a)) where
  getIxValues mb = [ maybe 0 unReference mb ]
  isReference _ = True

instance {-# OVERLAPPABLE #-} (Indexable a, Foldable f) => Indexable (f a) where
  getIxValues = foldMap getIxValues
  isReference _ = isReference (Proxy :: Proxy a)

instance Indexable Bool
instance Indexable Int
instance Indexable String

instance ToKey (Sortable a) => Indexable (Sortable a) where
  getIxValues s = [ toKey s ]
  isReference _ = False

instance (Hashable a, Indexable (Sortable a)) => Indexable (Unique (Sortable a)) where
  getUnique (Unique (Sortable a)) = Just . fromIntegral $ hash a
  getIxValues = getIxValues . unUnique
  isReference _ = isReference (Proxy :: Proxy (Sortable a))

instance {-# OVERLAPPABLE #-} Hashable a => Indexable (Unique a) where
  getUnique (Unique a) = Just . fromIntegral $ hash a

-- | Data type used by the key scrapper to collect keys while traversing
-- generically user types.
data Indexables = Indexables
  { ixReferences :: [(String, DocumentKey)]
  , ixSortables  :: [(String, SortableKey)]
  , ixUniques    :: [(String, UniqueKey)]
  } deriving (Show)

-- | Class used by the generic key scrapper to extract indexing information
-- from user types. Only an empty instance needs to be added in user code.
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
  ggetIndexables _ (M1 x) = ggetIndexables "" x

instance GetIndexables a => GetIndexables (M1 C c a) where
  ggetIndexables _ (M1 x) = ggetIndexables "" x

instance (GetIndexables a, Selector c) => GetIndexables (M1 S c a) where
  ggetIndexables _ m1@(M1 x) = ggetIndexables (selName m1) x

instance Indexable a => GetIndexables (K1 i a) where
  ggetIndexables n (K1 x) =
    if isReference (Proxy :: Proxy a)
    then Indexables { ixReferences = vs, ixSortables = [], ixUniques = us }
    else Indexables { ixReferences = [], ixSortables = vs, ixUniques = us }
    where vs = (\did -> (n, did)) <$> getIxValues x
          us = maybe [] (pure . (n,)) (getUnique x)
