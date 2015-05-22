{-# LANGUAGE MultiWayIf #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.IdSupply
-- Copyright   : (C) 2015 Călin Ardelean,
-- License     : MIT (see the file LICENSE.md)
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Unique ID allocation functions.
----------------------------------------------------------------------------

module Database.Muesli.IdSupply
  ( IdSupply
  , empty
  , reserve
  , alloc
  ) where

import           Control.Exception     (throw)
import           Data.IntMap.Strict    (IntMap)
import qualified Data.IntMap.Strict    as Map
import           Database.Muesli.Types (DBWord, DatabaseError (..))

type IdSupply = IntMap Size

type DID      = DBWord
type Size     = DBWord

empty :: IdSupply
empty = Map.singleton 1 (maxBound - 1)

reserve :: DID -> IdSupply -> IdSupply
reserve tidb s = maybe s
  (\(st, szdb) ->
      let sz = fromIntegral szdb in
      let delta = tidb - fromIntegral st in
      if | tid >= st + sz       -> s
         | tid == st && sz == 1 -> Map.delete st s
         | tid == st            -> Map.insert (tid + 1) (szdb - 1) $
                                   Map.delete st s
         | tid == st + sz - 1   -> Map.insert st (szdb - 1) s
         | otherwise            -> Map.insert (tid + 1) (szdb - delta - 1) $
                                   Map.insert st delta s)
  (Map.lookupLE tid s)
  where tid = fromIntegral tidb

alloc :: IdSupply -> (DID, IdSupply)
alloc s =
  case Map.lookupGE 0 s of
    Nothing -> throw $ IdAllocationError "ID allocation error: supply empty."
    Just (st, sz) ->
      let tid = fromIntegral st in
      if sz == 1 then (tid, Map.delete st s)
      else (tid, Map.insert (st + 1) (sz - 1) $ Map.delete st s)
