{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE TupleSections #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Cache
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- LRU cache implementation using the
-- <http://hackage.haskell.org/package/psqueues psqueues> package.
--
-- This module should be imported qualified.
----------------------------------------------------------------------------

module Database.Muesli.Cache
  ( DynValue (..)
  , LRUCache (..)
  , empty
  , insert
  , lookup
  , delete
  , trim
  ) where

import           Data.Dynamic    (Dynamic, fromDynamic, toDyn)
import           Data.IntPSQ     (IntPSQ)
import qualified Data.IntPSQ     as PQ
import           Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import           Data.Typeable   (Typeable)
import           Prelude         hiding (lookup)

-- | Holds a 'Dynamic' and the size of the corresponding serialized data.
data DynValue = DynValue
  { dynValue :: !Dynamic
  , dynSize  :: !Int
  } deriving (Show)

-- | A LRU cache that uses a lower capacity in periods of inactivity.
-- This behaviour would be useful for things like long lived Android services.
data LRUCache = LRUCache
  { minCapacity :: !Int -- ^ Minimum capacity under which 'maxAge' is ignored
  , maxCapacity :: !Int -- ^ Maximum capacity above which oldest items are removed
  , maxAge :: !NominalDiffTime
  , size   :: !Int      -- ^ Current size of the cache
  , queue  :: !(IntPSQ UTCTime DynValue)
  }

-- | Creates an empty cache.
empty :: Int             -- ^ Minimum capacity
      -> Int             -- ^ Maximum capacity
      -> NominalDiffTime -- ^ Maximum age
      -> LRUCache
empty minc maxc age = LRUCache { minCapacity = minc
                               , maxCapacity = maxc
                               , maxAge = age
                               , size   = 0
                               , queue  = PQ.empty
                               }

-- | Apply cache's policy and removes items if necessary.
trim :: UTCTime  -- ^ Current time
     -> LRUCache
     -> LRUCache
trim now c =
  if size c < minCapacity c then c
  else case PQ.findMin (queue c) of
    Nothing        -> c
    Just (_, p, v) ->
      if (size c < maxCapacity c) && (diffUTCTime now p < maxAge c)
      then c
      else trim now $! c { size  = size c - dynSize v
                         , queue = PQ.deleteMin (queue c)
                         }

-- | Adds a new item to the cache, and 'trim's.
insert :: Typeable a
       => UTCTime -- ^ Current time
       -> Int     -- ^ Key
       -> a       -- ^ Value
       -> Int     -- ^ Size
       -> LRUCache
       -> LRUCache
insert now k a sz c = trim now $! c { size  = size c + sz -
                                                   maybe 0 (dynSize . snd) mbv
                                    , queue = q
                                    }
  where (mbv, q) = PQ.insertView k now v (queue c)
        v = DynValue { dynValue = toDyn a
                     , dynSize  = sz
                     }

-- | Looks up an item into the cache.
-- If found, it updates the access time for the item, and then 'trim's.
lookup :: Typeable a
       => UTCTime -- ^ Current time
       -> Int     -- ^ Key
       -> LRUCache
       -> Maybe (a, Int, LRUCache)
lookup now k c =
  case PQ.alter f k (queue c) of
    (Nothing, _) -> Nothing
    (Just v,  q) -> (, dynSize v, c') <$> fromDynamic (dynValue v)
      where !c' = trim now $ c { queue = q }
  where f = maybe (Nothing, Nothing) (\(_, v) -> (Just v,  Just (now, v)))

-- | Deletes an item from the cache.
delete :: Int      -- ^ Key
       -> LRUCache
       -> LRUCache
delete k c = maybe c
  (\(_, v) -> c { size  = size c - dynSize v
                , queue = PQ.delete k (queue c)
                }) $
  PQ.lookup k (queue c)
