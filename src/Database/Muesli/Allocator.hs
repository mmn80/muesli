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
-- Document data allocator.
----------------------------------------------------------------------------

module Database.Muesli.Allocator
  ( empty
  , add
  , build
  , buildExtra
  , alloc
  ) where

import           Control.Exception             (throw)
import qualified Data.IntMap.Strict            as IntMap
import           Data.List                     (foldl', sortOn)
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromMaybe)
import           Database.Muesli.Backend.Types (LogRecord (..))
import           Database.Muesli.State         (GapsIndex, MainIndex)
import           Database.Muesli.Types         (DatabaseError (..), DocAddress,
                                                DocSize)

-- | Creates an index holding a single gap starting at the given address.
empty :: DocAddress -> GapsIndex
empty addr = Map.singleton (maxBound - addr) [addr]

-- | Adds a gap to the index.
add :: DocSize -> DocAddress -> GapsIndex -> GapsIndex
add sz addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs

-- | Builds the index from the 'LogRecord' data held in an 'MainIndex'.
--
-- __O(n*log(n))__ operation used by 'Database.Muesli.Handle.open' after
-- loading the log.
build :: MainIndex -> GapsIndex
build idx = addTail . foldl' f (Map.empty, 0) . sortOn recAddress .
            filter (not . recDeleted) . map head $ IntMap.elems idx
  where
    f (gs, addr) r = (gs', recAddress r + recSize r)
      where gs' = if addr == recAddress r then gs
                  else add sz addr gs
            sz = recAddress r - addr
    addTail (gs, addr) = add (maxBound - addr) addr gs

-- | Builds the index from a set of 'LogRecord's, with all space before the
-- given address considered reserved.
--
-- Used by the garbage collector (see module "Database.Muesli.GC").
buildExtra :: DocAddress -> [LogRecord] -> GapsIndex
buildExtra pos = foldl' f (empty pos)
  where f gs r = add (recSize r) (recAddress r) gs

-- | Allocates a new slot of the given size.
-- The smallest available size in the index is preffered.
--
-- Throws 'DataAllocationError' if no gap big enough is found.
alloc :: GapsIndex -> DocSize -> (DocAddress, GapsIndex)
alloc gs sz =
  case Map.lookupGE sz gs of
    Nothing -> throw $ DataAllocationError sz (fst <$> Map.lookupLT maxBound gs)
                       "Data allocation error."
    Just (gsz, a:as) ->
      if delta == 0 then (a, gs')
      else (a, add delta (a + sz) gs')
      where gs' = if null as then Map.delete gsz gs
                             else Map.insert gsz as gs
            delta = gsz - sz
