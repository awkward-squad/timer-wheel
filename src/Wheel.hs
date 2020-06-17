module Wheel
  ( Wheel(resolution)
  , create
  , lenMicros
  , insert
  , reap
  ) where

import Entries (Entries)

import qualified Entries as Entries

import Control.Concurrent      (threadDelay)
import Control.Concurrent.MVar
import Control.Monad           (join, when)
import Data.IORef
import Data.Vector             (Vector)
import Data.Word               (Word64)
import GHC.Clock               (getMonotonicTimeNSec)
import System.IO.Unsafe        (unsafeInterleaveIO)

import qualified Data.Vector as Vector



data Wheel
  = Wheel
  { buckets :: !(Vector (IORef Entries))
  , resolution :: !Word64 -- micros
  }

create ::
     Int
  -> Word64
  -> IO Wheel
create spokes resolution = do
  buckets :: Vector (IORef Entries) <-
    Vector.replicateM spokes (newIORef Entries.empty)

  pure Wheel
    { buckets = buckets
    , resolution = resolution
    }

numSpokes :: Wheel -> Int
numSpokes wheel =
  Vector.length (buckets wheel)

lenMicros :: Wheel -> Word64
lenMicros wheel =
  fromIntegral (numSpokes wheel) * resolution wheel

bucket :: Wheel -> Word64 -> IORef Entries
bucket wheel time =
  Vector.unsafeIndex
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
    bucketRef :: IORef Entries
    bucketRef =
      bucket wheel time

  atomicModifyIORef' bucketRef
    (\entries ->
      (Entries.insert key count action entries, ()))

  canceledVar :: MVar (Maybe Bool) <-
    unsafeInterleaveIO (newMVar Nothing)

  pure $ do
    modifyMVar canceledVar $ \result -> do
      canceled <-
        maybe
          (atomicModifyIORef' bucketRef
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
        (atomicModifyIORef' (Vector.unsafeIndex buckets i)
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
  (`div` 1000) <$> getMonotonicTimeNSec
