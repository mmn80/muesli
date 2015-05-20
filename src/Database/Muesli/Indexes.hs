-----------------------------------------------------------------------------
-- |
-- Module : Database.Muesli.Indexes
-- Copyright : (C) 2015 Călin Ardelean,
-- License : MIT (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides database indexes.
----------------------------------------------------------------------------

module Database.Muesli.Indexes
  ( updateMainIdx
  , updateUnqIdx
  , updateIntIdx
  , updateRefIdx
  ) where

import           Data.IntMap.Strict    (IntMap)
import qualified Data.IntMap.Strict    as Map
import qualified Data.IntSet           as Set
import           Data.List             (foldl')
import           Database.Muesli.State

updateMainIdx :: MainIndex -> [DocRecord] -> MainIndex
updateMainIdx = foldl' f
  where f idx r = let did = fromIntegral (docID r) in
                  let rs' = maybe [r] (r:) (Map.lookup did idx) in
                  Map.insert did rs' idx

updateUnqIdx :: UniqueIndex -> [DocRecord] -> UniqueIndex
updateUnqIdx = foldl' f
  where f idx r = foldl' g idx (docURefs r)
          where
            did = fromIntegral (docID r)
            del = docDel r
            g idx' ref =
              let rpid = fromIntegral (irefPID ref) in
              let rval = fromIntegral (irefVal ref) in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval did) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = if del then Map.delete rval is
                              else Map.insert rval did is

updateIntIdx :: SortIndex -> [DocRecord] -> SortIndex
updateIntIdx = foldl' f
  where f idx r = foldl' g idx (docIRefs r)
          where
            did = fromIntegral (docID r)
            del = docDel r
            g idx' ref =
              let rpid = fromIntegral (irefPID ref) in
              let rval = fromIntegral (irefVal ref) in
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

updateRefIdx :: FilterIndex -> [DocRecord] -> FilterIndex
updateRefIdx = foldl' f
  where f idx r = foldl' g idx (docDRefs r)
          where
            del = docDel r
            g idx' ref =
              let rpid = fromIntegral (drefPID ref) in
              let rval = fromIntegral (drefDID ref) in
              let sng = updateIntIdx Map.empty [r] in
              case Map.lookup rpid idx' of
                Nothing -> if del then idx'
                           else Map.insert rpid (Map.singleton rval sng) idx'
                Just is -> Map.insert rpid is' idx'
                  where is' = case Map.lookup rval is of
                                Nothing -> if del then is
                                           else Map.insert rval sng is
                                Just ss -> Map.insert rval ss' is
                                  where ss' = updateIntIdx ss [r]
