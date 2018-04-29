{-# language LambdaCase #-}

module Entries.List
  ( Entries
  , empty
  , Entries.List.null
  , size
  , insert
  , delete
  , squam
  ) where

import Data.List (partition)

newtype Entries
  = Entries [Entry]

data Entry = Entry
  { entryId :: {-# UNPACK #-} !Int
  , entryCount :: {-# UNPACK #-} !Int
  , entryAction :: IO ()
  }

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

insert :: Int -> Int -> IO () -> Entries -> Entries
insert i n m (Entries entries) =
  let
    entry :: Entry
    !entry =
      Entry i n m
  in
    Entries (entry : entries)
{-# INLINABLE insert #-}

delete :: Int -> Entries -> (Maybe (Entries -> Entries), Entries)
delete i (Entries xs0) =
  go [] xs0
 where
  go :: [Entry] -> [Entry] -> (Maybe (Entries -> Entries), Entries)
  go acc = \case
    [] ->
      (Nothing, Entries acc)
    x@(Entry j n m):xs
      | i == j ->
          (Just (insert j n m), Entries (acc ++ xs))
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
