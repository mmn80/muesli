-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Allocator
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Data file allocator.
----------------------------------------------------------------------------

module Database.Muesli.Allocator
  ( empty
  , add
  , build
  , buildExtra
  , alloc
  ) where

import           Control.Exception     (throw)
import           Data.IntMap.Strict    (IntMap)
import qualified Data.IntMap.Strict    as Map
import           Data.List             (foldl', sortOn)
import           Data.Maybe            (fromMaybe)
import           Database.Muesli.State

empty :: DocAddress -> GapsIndex
empty sz = Map.singleton (fromIntegral $ maxBound - sz) [sz]

add :: DocSize -> DocAddress -> GapsIndex -> GapsIndex
add s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = fromIntegral s

build :: MainIndex -> GapsIndex
build idx = addTail . foldl' f (Map.empty, 0) . sortOn recAddress .
                filter (not . recDeleted) . map head $ Map.elems idx
  where
    f (gs, addr) r = (gs', recAddress r + recSize r)
      where gs' = if addr == recAddress r then gs
                  else add sz addr gs
            sz = recAddress r - addr
    addTail (gs, addr) = add (maxBound - addr) addr gs

buildExtra :: DocAddress -> [LogRecord] -> GapsIndex
buildExtra pos = foldl' f (empty pos)
  where f gs r = add (recSize r) (recAddress r) gs

alloc :: GapsIndex -> DocSize -> (DocAddress, GapsIndex)
alloc gs s = let sz = fromIntegral s in
  case Map.lookupGE sz gs of
    Nothing -> throw $ DataAllocationError sz (fst <$> Map.lookupLT maxBound gs)
                       "Data allocation error."
    Just (gsz, a:as) ->
      if delta == 0 then (a, gs')
      else (a, add (fromIntegral delta) (a + fromIntegral sz) gs')
      where gs' = if null as then Map.delete gsz gs
                             else Map.insert gsz as gs
            delta = gsz - sz
