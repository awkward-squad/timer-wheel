module TimerWheel.Internal.Timers
  ( Timers (resolution),
    create,
    insert,
    reap,
  )
where

import Control.Monad (when)
import qualified Data.Atomics as Atomics
import Data.Primitive.Array (MutableArray)
import qualified Data.Primitive.Array as Array
import GHC.Base (RealWorld)
import TimerWheel.Internal.Entries (Entries)
import qualified TimerWheel.Internal.Entries as Entries
import TimerWheel.Internal.Micros (Micros (..))
import qualified TimerWheel.Internal.Micros as Micros
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp

data Timers = Timers
  { buckets :: {-# UNPACK #-} !(MutableArray RealWorld Entries),
    resolution :: {-# UNPACK #-} !Micros
  }

create :: Int -> Micros -> IO Timers
create spokes resolution = do
  buckets <- Array.newArray spokes Entries.empty
  pure Timers {buckets, resolution}

numSpokes :: Timers -> Int
numSpokes Timers {buckets} =
  Array.sizeofMutableArray buckets

lenMicros :: Timers -> Micros
lenMicros timers =
  Micros.scale (numSpokes timers) (resolution timers)

timestampToIndex :: Timers -> Timestamp -> Int
timestampToIndex timers@Timers {resolution} timestamp =
  fromIntegral (Timestamp.epoch resolution timestamp) `rem` numSpokes timers

insert :: Timers -> Int -> Micros -> IO () -> IO (IO Bool)
insert timers@Timers {buckets} key delay action = do
  now <- Timestamp.now
  let index = timestampToIndex timers (now `Timestamp.plus` delay)
  atomicInsertIntoBucket timers index key delay action
  pure (atomicDeleteFromBucket buckets index key)

reap :: Timers -> IO a
reap timers@Timers {buckets, resolution} = do
  now <- Timestamp.now
  let remainingBucketMicros = resolution `Micros.minus` (now `Timestamp.rem` resolution)
  Micros.sleep remainingBucketMicros
  loop (now `Timestamp.plus` remainingBucketMicros `Timestamp.plus` resolution) (timestampToIndex timers now)
  where
    loop :: Timestamp -> Int -> IO a
    loop nextTime index = do
      expired <- atomicExtractExpiredTimersFromBucket buckets index
      sequence_ expired
      afterTime <- Timestamp.now
      when (afterTime < nextTime) (Micros.sleep (nextTime `Timestamp.minus` afterTime))
      loop (nextTime `Timestamp.plus` resolution) ((index + 1) `rem` numSpokes timers)

------------------------------------------------------------------------------------------------------------------------
-- Atomic operations on buckets

atomicInsertIntoBucket :: Timers -> Int -> Int -> Micros -> IO () -> IO ()
atomicInsertIntoBucket timers@Timers {buckets} index key delay action = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket = do
      (success, ticket1) <- Atomics.casArrayElem buckets index ticket (doInsert (Atomics.peekTicket ticket))
      if success then pure () else loop ticket1

    doInsert :: Entries -> Entries
    doInsert =
      Entries.insert key (unMicros (delay `Micros.div` lenMicros timers)) action

atomicDeleteFromBucket :: MutableArray RealWorld Entries -> Int -> Int -> IO Bool
atomicDeleteFromBucket buckets index key = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket =
      case Entries.delete key (Atomics.peekTicket ticket) of
        Nothing -> pure False
        Just entries1 -> do
          (success, ticket1) <- Atomics.casArrayElem buckets index ticket entries1
          if success then pure True else loop ticket1

atomicExtractExpiredTimersFromBucket :: MutableArray RealWorld Entries -> Int -> IO [IO ()]
atomicExtractExpiredTimersFromBucket buckets index = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket
      | Entries.null entries = pure []
      | otherwise = do
          let (expired, entries1) = Entries.partition entries
          (success, ticket1) <- Atomics.casArrayElem buckets index ticket entries1
          if success then pure expired else loop ticket1
      where
        entries = Atomics.peekTicket ticket
