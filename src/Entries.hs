{-# language LambdaCase #-}

module Entries
  ( Entries
  , empty
  , Entries.null
  , size
  , insert
  , delete
  , squam
  ) where

import Entry

import Data.List (partition)

newtype Entries
  = Entries [Entry]

empty :: Entries
empty =
  Entries []

null :: Entries -> Bool
null = \case
  Entries [] ->
    True
  _ ->
    False
{-# INLINABLE null #-}

size :: Entries -> Int
size (Entries xs) =
  length xs
{-# INLINABLE size #-}

insert :: Entry -> Entries -> Entries
insert x (Entries xs) =
  Entries (x:xs)
{-# INLINABLE insert #-}

delete :: EntryId -> Entries -> (Maybe Entry, Entries)
delete i (Entries xs0) =
  go [] xs0
 where
  go :: [Entry] -> [Entry] -> (Maybe Entry, Entries)
  go acc = \case
    [] ->
      (Nothing, Entries acc)
    x:xs
      | i == entryId x ->
          (Just x, Entries (acc ++ xs))
      | otherwise ->
          go (x:acc) xs
{-# INLINABLE delete #-}

squam :: Entries -> ([IO ()], Entries)
squam (Entries entries) =
  let
    (expired, alive) =
      partition ((== 0) . entryCount) entries
  in
    (map entryAction expired, Entries (map' decrement alive))
{-# INLINABLE squam #-}

decrement :: Entry -> Entry
decrement entry =
  entry { entryCount = entryCount entry - 1 }

map' :: (a -> b) -> [a] -> [b]
map' f = \case
  [] ->
    []

  x:xs ->
    let
      !y = f x
    in
      y : map' f xs
