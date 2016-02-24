-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Indexes
-- Copyright   : (c) 2015-16 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Incremental database index update functions. Except for 'updateMainIndex',
-- these functions first search for the previous version for each record and,
-- if found, remove the old references from the index, then add the new ones.
--
-- Used during loading, query evaluation, and GC.
----------------------------------------------------------------------------

module Database.Muesli.Indexes
  ( updateMainIndex
  , updateFilterIndex
  , updateSortIndex
  , updateUniqueIndex
  ) where

import qualified Data.IntMap.Strict    as Map
import qualified Data.IntSet           as Set
import           Data.List             (foldl')
import           Database.Muesli.State

-- | Updates the 'MainIndex' (allocation table).
updateMainIndex :: MainIndex -> [LogRecord] -> MainIndex
updateMainIndex = foldl' f
  where f idx r = let did = fromIntegral (recDocumentKey r) in
                  let rs' = maybe [r] (r:) (Map.lookup did idx) in
                  Map.insert did rs' idx

getPreviousVersion :: MainIndex -> LogRecord -> Maybe LogRecord
getPreviousVersion idx r =
  let did = fromIntegral (recDocumentKey r) in
  maybe Nothing (\ors -> if null ors then Nothing
                         else Just (head ors) { recDeleted = True })
  (Map.lookup did idx)

-- | Updates the 'UniqueIndex'.
updateUniqueIndex :: MainIndex -> UniqueIndex -> [LogRecord] -> UniqueIndex
updateUniqueIndex mIdx = foldl' h
  where h idx r = f (maybe idx (f idx) (getPreviousVersion mIdx r)) r
        f idx r = foldl' g idx (recUniques r)
          where
            did = fromIntegral (recDocumentKey r)
            del = recDeleted r
            g idx' lnk =
              let rpid = fromIntegral (fst lnk) in
              let rval = fromIntegral (snd lnk) in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval did) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = if del then Map.delete rval is
                              else Map.insert rval did is

-- | Updates the main 'SortIndex', and also the 'SortIndex'es inside a 'FilterIndex'.
updateSortIndex :: MainIndex -> SortIndex -> [LogRecord] -> SortIndex
updateSortIndex mIdx = foldl' h
  where h idx r = f (maybe idx (f idx) (getPreviousVersion mIdx r)) r
        f idx r = foldl' g idx (recSortables r)
          where
            did = fromIntegral (recDocumentKey r)
            del = recDeleted r
            g idx' lnk =
              let rpid = fromIntegral (fst lnk) in
              let rval = fromIntegral (snd lnk) in
              let sng = Set.singleton did in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> if Set.null ss' then Map.delete rval is
                                           else Map.insert rval ss' is
                                  where ss' = if del then Set.delete did ss
                                              else Set.insert did ss

-- | Updates the 'FilterIndex'.
--
-- Calls 'updateSortIndex' for the internal sorted indexes.
updateFilterIndex :: MainIndex -> FilterIndex -> [LogRecord] -> FilterIndex
updateFilterIndex mIdx = foldl' h
  where h idx r = f (maybe idx (f idx) (getPreviousVersion mIdx r)) r
        f idx r = foldl' g idx (recReferences r)
          where
            del = recDeleted r
            g idx' lnk =
              let rpid = fromIntegral (fst lnk) in
              let rval = fromIntegral (snd lnk) in
              let sng = updateSortIndex mIdx Map.empty [r] in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> Map.insert rval ss' is
                                  where ss' = updateSortIndex mIdx ss [r]
