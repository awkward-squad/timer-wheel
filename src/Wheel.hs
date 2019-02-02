{-# LANGUAGE CPP #-}

module Wheel
  ( Wheel
  , new
  , lenMicros
  , bucket
  , reap
  ) where

import Entries (Entries)
import Utils

import qualified Entries as Entries

import Control.Concurrent           (threadDelay)
import Control.Monad                (join)
import Data.Foldable                (for_)
import Data.Primitive.MutVar        (MutVar, atomicModifyMutVar', newMutVar)
import Data.Primitive.UnliftedArray (MutableUnliftedArray, UnliftedArray,
                                     freezeUnliftedArray, indexUnliftedArray,
                                     sizeofUnliftedArray,
                                     unsafeNewUnliftedArray, writeUnliftedArray)
import Data.Word                    (Word64)
import GHC.Prim                     (RealWorld)


type IORef
  = MutVar RealWorld

data Wheel
  = Wheel
  { buckets :: !(UnliftedArray (IORef Entries))
  , resolution :: !Word64 -- micros
  }

new ::
     Int
  -> Word64 -- micros
  -> IO Wheel
new spokes resolution = do
  mbuckets :: MutableUnliftedArray RealWorld (IORef Entries) <-
    unsafeNewUnliftedArray spokes

  for_ [0 .. spokes-1] $ \i ->
    writeUnliftedArray mbuckets i =<< newMutVar Entries.empty

  buckets :: UnliftedArray (IORef Entries) <-
    freezeUnliftedArray mbuckets 0 spokes

  pure Wheel
    { buckets = buckets
    , resolution = resolution
    }

numSpokes :: Wheel -> Int
numSpokes wheel =
  sizeofUnliftedArray (buckets wheel)

lenMicros :: Wheel -> Word64
lenMicros wheel =
  fromIntegral (numSpokes wheel) * resolution wheel

bucket :: Wheel -> Word64 -> IORef Entries
bucket wheel time =
  indexUnliftedArray
    (buckets wheel)
    (index wheel time)

index :: Wheel -> Word64 -> Int
index wheel@Wheel{resolution} time =
  fromIntegral (time `div` resolution) `rem` numSpokes wheel

reap :: Wheel -> IO ()
reap wheel@Wheel{buckets, resolution} = do
  now :: Word64 <-
    getMonotonicMicros

  loop (index wheel now)

  where
    -- Reaper loop: we haven't yet run the entries at index 'i'.
    loop :: Int -> IO ()
    loop minIndex = do
      -- Sleep until the next bucket.
      threadDelay (fromIntegral resolution)

      now :: Word64 <-
        getMonotonicMicros

      -- Figure out which bucket we're in. Usually this will be 'minIndex+1',
      -- but maybe running the entries in bucket 'minIndex-1' took a long time
      -- and we slept all the way to 'minIndex+2'. In any case, we should run
      -- the entries in buckets up-to-but-not-including this one, beginning with
      -- bucket 'minIndex'.

      let
        maxIndex :: Int
        maxIndex =
          index wheel now

      let
        indices :: [Int]
        indices =
          if maxIndex > minIndex
            then [minIndex .. maxIndex - 1]
            else [minIndex .. numSpokes wheel - 1] ++ [0 .. maxIndex - 1]

      -- To actually run the entries in a bucket, partition them into
      -- expired (count == 0) and alive (count > 0). Run the expired entries
      -- and decrement the alive entries' counts by 1.

      for_ indices $ \i ->
        join
          (atomicModifyMutVar' (indexUnliftedArray buckets i)
            (\entries ->
              if Entries.null entries
                then
                  (entries, pure ())
                else
                  case Entries.partition entries of
                    (expired, alive) ->
                      (alive, sequence_ expired)))

      loop maxIndex
