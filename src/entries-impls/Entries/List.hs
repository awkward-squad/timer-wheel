{-# language LambdaCase #-}

module Entries.List
  ( Entries
  , empty
  , Entries.List.Common.null
  , size
  , insert
  , delete
  , squam
  ) where

import Entries.List.Common

import Data.Coerce
import Data.List (partition)
import Data.Word (Word64)

insert :: Int -> Word64 -> IO () -> Entries -> Entries
insert i n m =
  coerce (Entry i n m :)
{-# INLINABLE insert #-}

delete :: Int -> Entries -> Maybe Entries
delete =
  coerce delete_
{-# INLINABLE delete #-}

delete_ :: Int -> [Entry] -> Maybe [Entry]
delete_ i xs0 =
  go [] xs0
 where
  go :: [Entry] -> [Entry] -> Maybe [Entry]
  go acc = \case
    [] ->
      Nothing
    x:xs
      | i == entryId x ->
          Just (acc ++ xs)
      | otherwise ->
          go (x:acc) xs

squam :: Entries -> ([IO ()], Entries)
squam =
  coerce squam_
{-# INLINABLE squam #-}

squam_ :: [Entry] -> ([IO ()], [Entry])
squam_ =
  (\case
    (expired, alive) ->
      (map entryAction expired, decrement alive))
  . partition ((== 0) . entryCount)
