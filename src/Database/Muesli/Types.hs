{-# LANGUAGE DefaultSignatures          #-}
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
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Muesli markup types and typeclasses.
----------------------------------------------------------------------------

module Database.Muesli.Types
  (
-- * Indexable value wrappers
    Reference (..)
  , Sortable (..)
  , Unique (..)
-- * Main classes
  , Indexable (..)
  , Document (..)
  , Indexables (..)
-- * Index keyes
  , IxKey (..)
  , DocumentKey
  , SortableKey
  , UniqueKey
  , ToKey (..)
-- * Other
  , Property (..)
  , DatabaseError (..)
  ) where

import           Control.Exception     (Exception)
import           Control.Monad.Trans   (MonadIO)
import           Data.Bits             (Bits, FiniteBits)
import           Data.Hashable         (Hashable, hash)
import           Data.List             (foldl')
import           Data.Maybe            (fromJust)
import           Data.Serialize        (Serialize (..))
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word8)
import           Foreign               (Storable, sizeOf)
import           GHC.Generics          ((:*:) (..), (:+:) (..), C, D, Generic,
                                        K1 (..), M1 (..), Rep, S, Selector,
                                        U1 (..), from, selName)
import           Numeric               (showHex)

-- | Type for exceptions thrown by the database.
-- During normal operation these should never be thrown.
data DatabaseError
  -- | Thrown when the log file is corrupted.
  = LogParseError String
  -- | Thrown after deserialization errors.
  -- Holds starting position, size, and a message.
  | DataParseError Int Int String
  -- | ID allocation failure. For instance, full address space.
  | IdAllocationError String
  -- | Data allocation failure.
  -- Containes the size requested, the biggest available gap, and a message.
  | DataAllocationError Int (Maybe Int) String
  deriving (Show)

instance Exception DatabaseError

-- | Used inside indexes and as arguments to query primitives.
newtype IxKey = IxKey { unIxKey :: Int }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral,
            Bits, FiniteBits, Storable, Serialize)

instance Show IxKey where
  showsPrec _ (IxKey k) = showString "0x" . showHex k

class ToKey a where
  toKey :: a -> IxKey

type DocumentKey = IxKey
type SortableKey = IxKey
type UniqueKey   = IxKey

-- Properties ------------------------------------------------------------------

newtype Property a = Property { unProperty :: (IxKey, String) }

instance Eq (Property a) where
  Property (pid, _) == Property (pid', _) = pid == pid'

instance Show (Property a) where
  showsPrec p (Property (pid, s)) = showString s . showString "[" .
    showsPrec p pid . showString "]"

instance Typeable a => IsString (Property a) where
  fromString s = Property (pid, s)
    where pid = fromIntegral $ hash (show $ typeRep (Proxy :: Proxy a), s)

-- Values ----------------------------------------------------------------------

newtype Reference a = Reference { unReference :: IxKey }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral, Serialize)

instance Show (Reference a) where
  showsPrec p = showsPrec p . unReference

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

instance ToKey (Sortable UTCTime) where
  toKey (Sortable t) = round $ utcTimeToPOSIXSeconds t

instance {-# OVERLAPPABLE #-} Show a => ToKey (Sortable a) where
  toKey (Sortable a) = snd $ foldl' f (ws - 1, 0) bytes
    where bytes = (fromIntegral . fromEnum <$> take ws str) :: [Word8]
          f (n, v) b = (n - 1, if n >= 0 then v + fromIntegral b * 2 ^ (8 * n) else v)
          ws = sizeOf (0 :: IxKey)
          str = case show a of
                  '"':as -> as
                  ss     -> ss

newtype Unique a = Unique { unUnique :: a }
  deriving (Eq, Serialize)

instance Show a => Show (Unique a) where
  showsPrec p = showsPrec p . unUnique

instance Hashable a => ToKey (Unique (Sortable a)) where
  toKey (Unique (Sortable a)) = fromIntegral $ hash a

instance {-# OVERLAPPABLE #-} Hashable a => ToKey (Unique a) where
  toKey (Unique a) = fromIntegral $ hash a

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

-- Records ---------------------------------------------------------------------

data Indexables = Indexables
  { ixReferences :: [(String, DocumentKey)]
  , ixSortables  :: [(String, SortableKey)]
  , ixUniques    :: [(String, UniqueKey)]
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

instance Indexable a => GetIndexables (K1 i a) where
  ggetIndexables n (K1 x) =
    if isReference (Proxy :: Proxy a)
    then Indexables { ixReferences = vs, ixSortables = [], ixUniques = us }
    else Indexables { ixReferences = [], ixSortables = vs, ixUniques = us }
    where vs = (\did -> (n, did)) <$> getIxValues x
          us = maybe [] (pure . (n,)) (getUnique x)
