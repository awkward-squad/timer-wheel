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
import Data.Word (Word64)
import GHC.Base (RealWorld)
import TimerWheel.Internal.Entries (Entries)
import qualified TimerWheel.Internal.Entries as Entries
import TimerWheel.Internal.Micros (Micros (..))
import qualified TimerWheel.Internal.Micros as Micros
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp

data Timers = Timers
  { buckets :: {-# UNPACK #-} !(MutableArray RealWorld Entries),
    resolution :: {-# UNPACK #-} !Micros,
    -- The total number of microseconds that one trip 'round the wheel represents. This value is needed whenever a new
    -- timer is inserted, so we cache it, though this doesn't really show up on any benchmark we've put together heh
    totalMicros :: {-# UNPACK #-} !Micros
  }

create :: Int -> Micros -> IO Timers
create spokes resolution = do
  buckets <- Array.newArray spokes Entries.empty
  pure Timers {buckets, resolution, totalMicros}
  where
    totalMicros = Micros.scale spokes resolution

timestampToIndex :: MutableArray RealWorld Entries -> Micros -> Timestamp -> Int
timestampToIndex buckets resolution timestamp =
  -- This downcast is safe because there are at most `maxBound :: Int` buckets (not that anyone would ever have that
  -- many...)
  fromIntegral @Word64 @Int
    (Timestamp.epoch resolution timestamp `rem` fromIntegral @Int @Word64 (Array.sizeofMutableArray buckets))

insert :: Timers -> Int -> Micros -> IO () -> IO (IO Bool)
insert timers@Timers {buckets, resolution} key delay action = do
  now <- Timestamp.now
  let index = timestampToIndex buckets resolution (now `Timestamp.plus` delay)
  atomicInsertIntoBucket timers index key delay action
  pure (atomicDeleteFromBucket buckets index key)

reap :: Timers -> IO a
reap Timers {buckets, resolution} = do
  now <- Timestamp.now
  let remainingBucketMicros = resolution `Micros.minus` (now `Timestamp.rem` resolution)
  Micros.sleep remainingBucketMicros
  loop
    (now `Timestamp.plus` remainingBucketMicros `Timestamp.plus` resolution)
    (timestampToIndex buckets resolution now)
  where
    loop :: Timestamp -> Int -> IO a
    loop nextTime index = do
      expired <- atomicExtractExpiredTimersFromBucket buckets index
      sequence_ expired
      afterTime <- Timestamp.now
      when (afterTime < nextTime) (Micros.sleep (nextTime `Timestamp.minus` afterTime))
      loop (nextTime `Timestamp.plus` resolution) ((index + 1) `rem` Array.sizeofMutableArray buckets)

------------------------------------------------------------------------------------------------------------------------
-- Atomic operations on buckets

atomicInsertIntoBucket :: Timers -> Int -> Int -> Micros -> IO () -> IO ()
atomicInsertIntoBucket Timers {buckets, totalMicros} index key delay action = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket = do
      (success, ticket1) <- Atomics.casArrayElem buckets index ticket (doInsert (Atomics.peekTicket ticket))
      if success then pure () else loop ticket1

    doInsert :: Entries -> Entries
    doInsert =
      Entries.insert key (unMicros (delay `Micros.div` totalMicros)) action

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
