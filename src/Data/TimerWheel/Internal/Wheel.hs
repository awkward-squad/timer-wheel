module Data.TimerWheel.Internal.Wheel
  ( Wheel (resolution),
    create,
    lenMicros,
    insert,
    reap,
  )
where

import Control.Monad (join, replicateM, when)
import Data.Array (Array)
import qualified Data.Array as Array
import Data.IORef
import Data.TimerWheel.Internal.Entries (Entries)
import qualified Data.TimerWheel.Internal.Entries as Entries
import Data.TimerWheel.Internal.Micros (Micros (..))
import qualified Data.TimerWheel.Internal.Micros as Micros
import Data.TimerWheel.Internal.Timestamp (Timestamp)
import qualified Data.TimerWheel.Internal.Timestamp as Timestamp

data Wheel = Wheel
  { buckets :: {-# UNPACK #-} !(Array Int (IORef Entries)),
    resolution :: {-# UNPACK #-} !Micros
  }

create :: Int -> Micros -> IO Wheel
create spokes resolution = do
  refs <- replicateM spokes (newIORef Entries.empty)
  let buckets = Array.listArray (0, spokes - 1) refs
  pure Wheel {buckets, resolution}

numSpokes :: Wheel -> Int
numSpokes wheel =
  length (buckets wheel)

lenMicros :: Wheel -> Micros
lenMicros wheel =
  Micros.scale (numSpokes wheel) (resolution wheel)

bucket :: Wheel -> Timestamp -> IORef Entries
bucket wheel timestamp =
  buckets wheel Array.! index wheel timestamp

index :: Wheel -> Timestamp -> Int
index wheel@Wheel {resolution} timestamp =
  fromIntegral (Timestamp.epoch resolution timestamp) `rem` numSpokes wheel

insert :: Wheel -> Int -> Micros -> IO () -> IO (IO Bool)
insert wheel key delay action = do
  bucketRef <- do
    now <- Timestamp.now
    pure (bucket wheel (now `Timestamp.plus` delay))

  atomicModifyIORef' bucketRef (\entries -> (insertEntry entries, ()))

  pure do
    atomicModifyIORef' bucketRef \entries ->
      case Entries.delete key entries of
        Nothing -> (entries, False)
        Just entries' -> (entries', True)
  where
    insertEntry :: Entries -> Entries
    insertEntry =
      Entries.insert key (unMicros (delay `Micros.div` lenMicros wheel)) action

reap :: Wheel -> IO a
reap wheel@Wheel {buckets, resolution} = do
  now <- Timestamp.now
  let remainingBucketMicros = resolution `Micros.minus` (now `Timestamp.rem` resolution)
  Micros.sleep remainingBucketMicros
  loop (now `Timestamp.plus` remainingBucketMicros `Timestamp.plus` resolution) (index wheel now)
  where
    loop :: Timestamp -> Int -> IO a
    loop nextTime i = do
      join (atomicModifyIORef' (buckets Array.! i) expire)
      afterTime <- Timestamp.now
      when (afterTime < nextTime) (Micros.sleep (nextTime `Timestamp.minus` afterTime))
      loop (nextTime `Timestamp.plus` resolution) ((i + 1) `rem` numSpokes wheel)
    expire :: Entries -> (Entries, IO ())
    expire entries
      | Entries.null entries = (entries, pure ())
      | otherwise = (alive, sequence_ expired)
      where
        (expired, alive) = Entries.partition entries
