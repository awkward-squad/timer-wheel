{-# language LambdaCase #-}
{-# language TypeApplications #-}

module Entries.PSQ
  ( Entries
  , empty
  , Entries.PSQ.null
  , size
  , insert
  , delete
  , squam
  ) where

import Data.Coerce
import Data.IntPSQ (IntPSQ)
import Data.Word (Word64)

import qualified Data.IntPSQ as IntPSQ

newtype Entries
  = Entries (IntPSQ Word64 (IO ()))

empty :: Entries
empty =
  Entries IntPSQ.empty
{-# INLINABLE empty #-}

null :: Entries -> Bool
null =
  coerce (IntPSQ.null @Word64 @(IO ()))
{-# INLINABLE null #-}

size :: Entries -> Int
size =
  coerce (IntPSQ.size @Word64 @(IO ()))
{-# INLINABLE size #-}

insert :: Int -> Word64 -> IO () -> Entries -> Entries
insert i n m =
  coerce (IntPSQ.unsafeInsertNew i n m)
{-# INLINABLE insert #-}

delete :: Int -> Entries -> Maybe Entries
delete =
  coerce delete_
{-# INLINABLE delete #-}

delete_ :: Int -> IntPSQ Word64 (IO ()) -> Maybe (IntPSQ Word64 (IO ()))
delete_ i xs =
  (\(_, _, ys) -> ys) <$> IntPSQ.deleteView i xs

squam :: Entries -> ([IO ()], Entries)
squam (Entries entries) =
  case IntPSQ.atMostView 0 entries of
    (expired, alive) ->
      (map f expired, Entries (IntPSQ.unsafeMapMonotonic g alive))
 where
  f :: (Int, Word64, IO ()) -> IO ()
  f (_, _, m) =
    m

  g :: Int -> Word64 -> IO () -> (Word64, IO ())
  g _ n m =
    (n-1, m)
{-# INLINABLE squam #-}
