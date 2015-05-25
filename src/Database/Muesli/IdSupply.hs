{-# LANGUAGE MultiWayIf #-}

{-# OPTIONS_HADDOCK show-extensions #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.IdSupply
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Unique 'DocumentKey' allocation functions.
--
-- Since we use 'IntMap's for our indexes, a (faster) auto-incremented key
-- will be exhausted on 32 bit machines before we reach a 'maxBound' number
-- of documents, because deleted keys cannot be reused.
----------------------------------------------------------------------------

module Database.Muesli.IdSupply
  ( IdSupply
  , empty
  , reserve
  , alloc
  ) where

import           Control.Exception             (throw)
import           Data.IntMap.Strict            (IntMap)
import qualified Data.IntMap.Strict            as Map
import           Database.Muesli.Types         (DatabaseError (..), DocumentKey)

-- | A map from keys to gap sizes.
type IdSupply = IntMap Int

-- | Holds a single gap of size 'maxBound' - 1, starting at address 1.
empty :: IdSupply
empty = Map.singleton 1 (maxBound - 1)

-- | Removes a key from the supply. Used during log loading.
reserve :: DocumentKey -> IdSupply -> IdSupply
reserve didb s = maybe s
  (\(st, sz) ->
      let delta = did - st in
      if | did >= st + sz       -> s
         | did == st && sz == 1 -> Map.delete st s
         | did == st            -> Map.insert (did + 1) (sz - 1) $
                                   Map.delete st s
         | did == st + sz - 1   -> Map.insert st (sz - 1) s
         | otherwise            -> Map.insert (did + 1) (sz - delta - 1) $
                                   Map.insert st delta s)
  (Map.lookupLE did s)
  where did = fromIntegral didb

-- | Allocates a fresh key from the supply.
--
-- Favours smallest numbers. For instance, after a document is deleted and
-- garbage collected, the 'DocumentKey' of that document typically becomes
-- the smallest, and thus the first available.
-- For this reason, 'IdSupply' will normally be small and efficient.
alloc :: IdSupply -> (DocumentKey, IdSupply)
alloc s =
  case Map.lookupGE 0 s of
    Nothing -> throw $ IdAllocationError "Key allocation error: supply empty."
    Just (st, sz) ->
      let did = fromIntegral st in
      if sz == 1 then (did, Map.delete st s)
      else (did, Map.insert (st + 1) (sz - 1) $ Map.delete st s)
