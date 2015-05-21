{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE TupleSections #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Cache
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- LRU cache implementation.
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

data DynValue = DynValue
  { dynValue :: !Dynamic
  , dynSize  :: !Int
  } deriving (Show)

data LRUCache = LRUCache
  { cMinCap :: !Int
  , cMaxCap :: !Int
  , cMaxAge :: !NominalDiffTime
  , cSize   :: !Int
  , cQueue  :: !(IntPSQ UTCTime DynValue)
  }

empty :: Int -> Int -> NominalDiffTime -> LRUCache
empty minc maxc age = LRUCache { cMinCap = minc
                               , cMaxCap = maxc
                               , cMaxAge = age
                               , cSize   = 0
                               , cQueue  = PQ.empty
                               }

trim :: UTCTime -> LRUCache -> LRUCache
trim now c =
  if cSize c < cMinCap c then c
  else case PQ.findMin (cQueue c) of
    Nothing        -> c
    Just (_, p, v) -> if (cSize c < cMaxCap c) && (diffUTCTime now p < cMaxAge c)
                      then c
                      else trim now $! c { cSize  = cSize c - dynSize v
                                         , cQueue = PQ.deleteMin (cQueue c)
                                         }

insert :: Typeable a => UTCTime -> Int -> a -> Int -> LRUCache -> LRUCache
insert now k a sz c = trim now $! c { cSize  = cSize c + sz - maybe 0 (dynSize . snd) mbv
                                    , cQueue = q
                                    }
  where (mbv, q) = PQ.insertView k now v (cQueue c)
        v = DynValue { dynValue = toDyn a
                     , dynSize  = sz
                     }

lookup :: Typeable a => UTCTime -> Int -> LRUCache -> Maybe (a, Int, LRUCache)
lookup now k c =
  case PQ.alter f k (cQueue c) of
    (Nothing, _) -> Nothing
    (Just v,  q) -> (, dynSize v, c') <$> fromDynamic (dynValue v)
      where !c' = trim now $ c { cQueue = q }
  where f = maybe (Nothing, Nothing) (\(_, v) -> (Just v,  Just (now, v)))

delete :: Int -> LRUCache -> LRUCache
delete k c = maybe c
  (\(_, v) -> c { cSize  = cSize c - dynSize v
                , cQueue = PQ.delete k (cQueue c)
                }) $
  PQ.lookup k (cQueue c)
