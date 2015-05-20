{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeOperators              #-}

-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Types
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides Muesli markup types and typeclasses.
----------------------------------------------------------------------------

module Database.Muesli.Types
  ( DBWord (..)
  , dbWordSize
  , Property (..)
  , DocID (..)
  , IntVal (..)
  , intVal
  , intValUnique
  , Unique (..)
  , Indexable (..)
  , DBValue (..)
  , Indexables (..)
  , Document (..)
  , DatabaseError (..)
  ) where

import           Control.Exception     (Exception)
import           Data.Bits             (Bits, FiniteBits (..))
import           Data.Hashable         (Hashable, hash)
import           Data.List             (foldl')
import           Data.Maybe            (fromJust)
import           Data.Serialize        (Serialize (..))
import           Data.Serialize.Get    (getWord32be)
import           Data.Serialize.Put    (putWord32be)
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word32, Word8)
import           GHC.Generics          ((:*:) (..), (:+:) (..), C, D, Generic,
                                        K1 (..), M1 (..), Rep, S, Selector,
                                        U1 (..), from, selName)
import           Numeric               (showHex)

data DatabaseError = LogParseError Int String
                   | DataParseError Int Int String
                   | IdAllocationError String
                   | DataAllocationError Int (Maybe Int) String
  deriving (Show)

instance Exception DatabaseError

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral, Bits, FiniteBits)

instance Serialize DBWord where
  put = putWord32be . unDBWord
  get = DBWord <$> getWord32be

instance Show DBWord where
  showsPrec p = showsPrec p . unDBWord

dbWordSize :: Int
dbWordSize = finiteBitSize (0 :: DBWord) `div` 8

type PropID   = DBWord
type DID      = DBWord
type IntValue = DBWord

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

-- Values ----------------------------------------------------------------------

newtype DocID a = DocID { unDocID :: DID }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral, Serialize)

instance Show (DocID a) where
  showsPrec _ (DocID k) = showString "0x" . showHex k

newtype IntVal a = IntVal { unIntVal :: DID }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral)

instance Show (IntVal a) where
  showsPrec _ (IntVal k) = showString "0x" . showHex k

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
  getDBValues (Indexable a) = [ snd $ foldl' f (dbWordSize - 1, 0) bytes ]
    where bytes = (fromIntegral . fromEnum <$> take dbWordSize (show a)) :: [Word8]
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
