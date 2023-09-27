module TimerWheel.Internal.Timers
  ( Timers (resolution),
    create,
    lenMicros,
    insert,
    reap,
  )
where

import Control.Monad (join, when)
import Data.Foldable (for_)
import Data.IORef
import Data.Primitive.Array (Array)
import qualified Data.Primitive.Array as Array
import TimerWheel.Internal.Entries (Entries)
import qualified TimerWheel.Internal.Entries as Entries
import TimerWheel.Internal.Micros (Micros (..))
import qualified TimerWheel.Internal.Micros as Micros
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp

data Timers = Timers
  { buckets :: {-# UNPACK #-} !(Array (IORef Entries)),
    resolution :: {-# UNPACK #-} !Micros
  }

create :: Int -> Micros -> IO Timers
create spokes resolution = do
  mbuckets <- Array.newArray spokes undefined
  for_ [0 .. spokes - 1] \i -> newIORef Entries.empty >>= Array.writeArray mbuckets i
  buckets <- Array.unsafeFreezeArray mbuckets
  pure Timers {buckets, resolution}

numSpokes :: Timers -> Int
numSpokes timers =
  length (buckets timers)

lenMicros :: Timers -> Micros
lenMicros timers =
  Micros.scale (numSpokes timers) (resolution timers)

bucket :: Timers -> Timestamp -> IORef Entries
bucket timers timestamp =
  Array.indexArray (buckets timers) (index timers timestamp)

index :: Timers -> Timestamp -> Int
index timers@Timers {resolution} timestamp =
  fromIntegral (Timestamp.epoch resolution timestamp) `rem` numSpokes timers

insert :: Timers -> Int -> Micros -> IO () -> IO (IO Bool)
insert timers key delay action = do
  bucketRef <- do
    now <- Timestamp.now
    pure (bucket timers (now `Timestamp.plus` delay))

  atomicModifyIORef' bucketRef (\entries -> (insertEntry entries, ()))

  pure do
    atomicModifyIORef' bucketRef \entries ->
      case Entries.delete key entries of
        Nothing -> (entries, False)
        Just entries' -> (entries', True)
  where
    insertEntry :: Entries -> Entries
    insertEntry =
      Entries.insert key (unMicros (delay `Micros.div` lenMicros timers)) action

reap :: Timers -> IO a
reap timers@Timers {buckets, resolution} = do
  now <- Timestamp.now
  let remainingBucketMicros = resolution `Micros.minus` (now `Timestamp.rem` resolution)
  Micros.sleep remainingBucketMicros
  loop (now `Timestamp.plus` remainingBucketMicros `Timestamp.plus` resolution) (index timers now)
  where
    loop :: Timestamp -> Int -> IO a
    loop nextTime i = do
      join (atomicModifyIORef' (Array.indexArray buckets i) expire)
      afterTime <- Timestamp.now
      when (afterTime < nextTime) (Micros.sleep (nextTime `Timestamp.minus` afterTime))
      loop (nextTime `Timestamp.plus` resolution) ((i + 1) `rem` numSpokes timers)

    expire :: Entries -> (Entries, IO ())
    expire entries
      | Entries.null entries = (entries, pure ())
      | otherwise = (alive, sequence_ expired)
      where
        (expired, alive) = Entries.partition entries
