{-# language LambdaCase #-}

module Entries.PSQ
  ( Entries
  , empty
  , Entries.PSQ.null
  , size
  , insert
  , delete
  , squam
  ) where

import Data.IntPSQ (IntPSQ)

import qualified Data.IntPSQ as IntPSQ

newtype Entries
  = Entries (IntPSQ Int (IO ()))

empty :: Entries
empty =
  Entries IntPSQ.empty
{-# INLINABLE empty #-}

null :: Entries -> Bool
null (Entries xs) =
  IntPSQ.null xs
{-# INLINABLE null #-}

size :: Entries -> Int
size (Entries xs) =
  IntPSQ.size xs
{-# INLINABLE size #-}

insert :: Int -> Int -> IO () -> Entries -> Entries
insert i n m (Entries xs) =
  Entries (IntPSQ.insert i n m xs)
{-# INLINABLE insert #-}

delete :: Int -> Entries -> (Maybe (Entries -> Entries), Entries)
delete i (Entries xs) =
  case IntPSQ.alter f i xs of
    (entry, xs') ->
      (entry, Entries xs')
 where
  f :: Maybe (Int, IO ()) -> (Maybe (Entries -> Entries), Maybe (Int, IO ()))
  f = \case
    Nothing ->
      (Nothing, Nothing)
    Just (n, m)  ->
      (Just (insert i n m), Nothing)
{-# INLINABLE delete #-}

squam :: Entries -> ([IO ()], Entries)
squam (Entries entries) =
  case IntPSQ.atMostView 0 entries of
    (expired, alive) ->
      (map f expired, Entries (IntPSQ.unsafeMapMonotonic g alive))
 where
  f :: (Int, Int, IO ()) -> IO ()
  f (_, _, m) =
    m

  g :: Int -> Int -> IO () -> (Int, IO ())
  g _ n m =
    (n-1, m)
{-# INLINABLE squam #-}
