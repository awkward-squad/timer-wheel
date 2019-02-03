{-# LANGUAGE CPP #-}

module Wheel
  ( Wheel(resolution)
  , create
  , lenMicros
  , insert
  , reap
  ) where

import Entries (Entries)

import qualified Entries as Entries

import Control.Concurrent           (threadDelay)
import Control.Concurrent.MVar
import Control.Monad                (join, when)
import Data.Foldable                (for_)
import Data.Primitive.MutVar        (MutVar, atomicModifyMutVar', newMutVar)
import Data.Primitive.UnliftedArray (MutableUnliftedArray, UnliftedArray,
                                     freezeUnliftedArray, indexUnliftedArray,
                                     sizeofUnliftedArray,
                                     unsafeNewUnliftedArray, writeUnliftedArray)
import Data.Word                    (Word64)
import GHC.Prim                     (RealWorld)
import System.IO.Unsafe             (unsafeInterleaveIO)

#if MIN_VERSION_base(4,11,0)
import GHC.Clock (getMonotonicTimeNSec)
#else
import System.Clock (Clock(Monotonic), getTime, toNanoSecs)
#endif



type IORef
  = MutVar RealWorld

data Wheel
  = Wheel
  { buckets :: !(UnliftedArray (IORef Entries))
  , resolution :: !Word64 -- micros
  }

create ::
     Int
  -> Word64
  -> IO Wheel
create spokes resolution = do
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

insert ::
     Wheel
  -> Int
  -> IO ()
  -> Word64
  -> IO (IO Bool)
insert wheel key action delay = do
  now :: Word64 <-
    getMonotonicMicros

  let
    time :: Word64
    time =
      now + delay

  let
    count :: Word64
    count =
      delay `div` lenMicros wheel

  let
    bucketVar :: MutVar RealWorld Entries
    bucketVar =
      bucket wheel time

  atomicModifyMutVar' bucketVar
    (\entries ->
      (Entries.insert key count action entries, ()))

  canceledVar :: MVar (Maybe Bool) <-
    unsafeInterleaveIO (newMVar Nothing)

  pure $ do
    modifyMVar canceledVar $ \result -> do
      canceled <-
        maybe
          (atomicModifyMutVar' bucketVar
            (\entries ->
              maybe
                (entries, False)
                (, True)
                (Entries.delete key entries)))
          pure
          result
      pure (Just canceled, canceled)


reap :: Wheel -> IO ()
reap wheel@Wheel{buckets, resolution} = do
  now :: Word64 <-
    getMonotonicMicros

  let
    -- How far in time are we into the very first bucket? Sleep until it's over.
    elapsedBucketMicros :: Word64
    elapsedBucketMicros =
      now `rem` resolution

  let
    remainingBucketMicros :: Word64
    remainingBucketMicros =
      resolution - elapsedBucketMicros

  threadDelay (fromIntegral remainingBucketMicros)

  loop
    (now + remainingBucketMicros + resolution)
    (index wheel now)

  where
    loop :: Word64 -> Int -> IO ()
    loop nextTime i = do
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

      afterTime :: Word64 <-
        getMonotonicMicros

      when (afterTime < nextTime) $
        (threadDelay (fromIntegral (nextTime - afterTime)))

      loop (nextTime + resolution) ((i+1) `rem` numSpokes wheel)

getMonotonicMicros :: IO Word64
getMonotonicMicros =
#if MIN_VERSION_base(4,11,0)
  (`div` 1000) <$> getMonotonicTimeNSec
#else
  ((`div` 1000) . fromIntegral . toNanoSecs <$> getTime Monotonic)
#endif
