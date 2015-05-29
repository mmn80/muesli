-----------------------------------------------------------------------------
-- |
-- Module      : Database.Muesli.Indexes
-- Copyright   : (c) 2015 Călin Ardelean
-- License     : MIT
--
-- Maintainer  : Călin Ardelean <calinucs@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Incremental database index update functions.
--
-- Used during loading, query evaluation, and GC.
----------------------------------------------------------------------------

module Database.Muesli.Indexes
  ( updateMainIdx
  , updateRefIdx
  , updateSortIdx
  , updateUniquenqIdx
  ) where

import qualified Data.IntMap.Strict    as Map
import qualified Data.IntSet           as Set
import           Data.List             (foldl')
import           Database.Muesli.State

-- | Updates the 'MainIndex' (allocation table).
updateMainIdx :: MainIndex -> [LogRecord] -> MainIndex
updateMainIdx = foldl' f
  where f idx r = let did = fromIntegral (recDocumentKey r) in
                  let rs' = maybe [r] (r:) (Map.lookup did idx) in
                  Map.insert did rs' idx

-- | Updates the 'UniqueIndex'.
updateUniquenqIdx :: UniqueIndex -> [LogRecord] -> UniqueIndex
updateUniquenqIdx = foldl' f
  where f idx r = foldl' g idx (recUniques r)
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
updateSortIdx :: SortIndex -> [LogRecord] -> SortIndex
updateSortIdx = foldl' f
  where f idx r = foldl' g idx (recSortables r)
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
-- Calls 'updateSortIdx' for the internal sorted indexes.
updateRefIdx :: FilterIndex -> [LogRecord] -> FilterIndex
updateRefIdx = foldl' f
  where f idx r = foldl' g idx (recReferences r)
          where
            del = recDeleted r
            g idx' lnk =
              let rpid = fromIntegral (fst lnk) in
              let rval = fromIntegral (snd lnk) in
              let sng = updateSortIdx Map.empty [r] in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> Map.insert rval ss' is
                                  where ss' = updateSortIdx ss [r]
