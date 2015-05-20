-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Allocator
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the data allocator.
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

empty :: Addr -> Gaps
empty sz = Map.singleton (fromIntegral $ maxBound - sz) [sz]

add :: Size -> Addr -> Gaps -> Gaps
add s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = fromIntegral s

build :: MainIndex -> Gaps
build idx = addTail . foldl' f (Map.empty, 0) . sortOn docAddr .
                filter (not . docDel) . map head $ Map.elems idx
  where
    f (gs, addr) r = (gs', docAddr r + docSize r)
      where gs' = if addr == docAddr r then gs
                  else add sz addr gs
            sz = docAddr r - addr
    addTail (gs, addr) = add (maxBound - addr) addr gs

buildExtra :: Addr -> [DocRecord] -> Gaps
buildExtra pos = foldl' f (empty pos)
  where f gs r = add (docSize r) (docAddr r) gs

alloc :: Gaps -> Size -> (Addr, Gaps)
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
