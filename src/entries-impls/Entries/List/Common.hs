{-# language LambdaCase #-}

module Entries.List.Common
  ( Entries(..)
  , Entry(..)
  , empty
  , Entries.List.Common.null
  , size
  , decrement
  ) where

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
null (Entries xs) =
  Prelude.null xs
{-# INLINABLE null #-}

size :: Entries -> Int
size (Entries xs) =
  length xs
{-# INLINABLE size #-}

decrement :: [Entry] -> [Entry]
decrement =
  map' decrement1

decrement1 :: Entry -> Entry
decrement1 entry =
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
