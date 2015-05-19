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
  ( emptyGaps
  , addGap
  , buildGaps
  , alloc
  ) where

import           Control.Exception        (throw)
import           Data.IntMap.Strict       (IntMap)
import qualified Data.IntMap.Strict       as Map
import           Data.List                (filter, foldl', sortOn)
import           Data.Maybe               (fromMaybe)
import           Database.Muesli.Internal

emptyGaps :: Addr -> IntMap [Addr]
emptyGaps sz = Map.singleton (fromIntegral $ maxBound - sz) [sz]

addGap :: Size -> Addr -> IntMap [Addr] -> IntMap [Addr]
addGap s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = fromIntegral s

buildGaps :: IntMap [DocRecord] -> IntMap [Addr]
buildGaps idx = addTail . foldl' f (Map.empty, 0) . sortOn docAddr .
                filter (not . docDel) . map head $ Map.elems idx
  where
    f (gs, addr) r = (gs', docAddr r + docSize r)
      where gs' = if addr == docAddr r then gs
                  else addGap sz addr gs
            sz = docAddr r - addr
    addTail (gs, addr) = addGap (maxBound - addr) addr gs

alloc :: IntMap [Addr] -> Size -> (Addr, IntMap [Addr])
alloc gs s = let sz = fromIntegral s in
  case Map.lookupGE sz gs of
    Nothing -> throw $ DataAllocationError sz (fst <$> Map.lookupLT maxBound gs)
                       "Data allocation error."
    Just (gsz, a:as) ->
      if delta == 0 then (a, gs')
      else (a, addGap (fromIntegral delta) (a + fromIntegral sz) gs')
      where gs' = if null as then Map.delete gsz gs
                             else Map.insert gsz as gs
            delta = gsz - sz
