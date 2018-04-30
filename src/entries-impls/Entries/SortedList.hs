{-# language LambdaCase #-}

module Entries.SortedList
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
import Data.Word (Word64)

insert :: Int -> Word64 -> IO () -> Entries -> Entries
insert i n m =
  coerce (insert_ (Entry i n m))
{-# INLINABLE insert #-}

insert_ :: Entry -> [Entry] -> [Entry]
insert_ entry@(Entry _ n _) =
  loop
 where
  loop :: [Entry] -> [Entry]
  loop = \case
    [] ->
      [entry]
    ess@(e:es)
      | n <= entryCount e ->
          entry : ess
      | otherwise ->
          e : loop es

delete :: Int -> Entries -> (Maybe (Entries -> Entries), Entries)
delete =
  coerce delete_
{-# INLINABLE delete #-}

delete_ :: Int -> [Entry] -> (Maybe ([Entry] -> [Entry]), [Entry])
delete_ i = \case
  [] ->
    (Nothing, [])
  e:es
    | i == entryId e ->
        (Just (insert_ e), es)
    | otherwise ->
        case delete_ i es of
          (f, xs) ->
            (f, e:xs)

squam :: Entries -> ([IO ()], Entries)
squam =
  coerce squam_
{-# INLINABLE squam #-}

squam_ :: [Entry] -> ([IO ()], [Entry])
squam_ =
  (\case
    (expired, alive) ->
      (map entryAction expired, decrement alive))
  . span ((== 0) . entryCount)
