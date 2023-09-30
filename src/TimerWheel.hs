module TimerWheel
  ( -- * Timer wheel
    TimerWheel,
    Config (..),
    Seconds,

    -- ** Constructing a timer wheel
    create,
    with,

    -- ** Querying a timer wheel
    count,

    -- ** Registering timers in a timer wheel
    register,
    register_,
    recurring,
    recurring_,
  )
where

import qualified Data.Atomics as Atomics
import Data.Primitive.Array (MutableArray)
import qualified Data.Primitive.Array as Array
import GHC.Base (RealWorld)
import qualified Ki
import TimerWheel.Internal.Counter (Counter, decrCounter_, incrCounter, incrCounter_, newCounter, readCounter)
import TimerWheel.Internal.Nanoseconds (Nanoseconds (..))
import qualified TimerWheel.Internal.Nanoseconds as Nanoseconds
import TimerWheel.Internal.Prelude
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp
import TimerWheel.Internal.TimestampMap (TimestampMap)
import qualified TimerWheel.Internal.TimestampMap as TimestampMap

-- | A timer wheel is a vector-of-collections-of timers to fire. It is configured with a /spoke count/ and /resolution/.
-- Timers may be scheduled arbitrarily far in the future. A timeout thread is spawned to step through the timer wheel
-- and fire expired timers at regular intervals.
--
-- * The /spoke count/ determines the size of the timer vector.
--
--     * A __larger spoke count__ will result in __less insert contention__ at each spoke and will require
--       __more memory__ to store the timer wheel.
--
--     * A __smaller spoke count__ will result in __more insert contention__ at each spoke and will require
--       __less memory__ to store the timer wheel.
--
-- * The /resolution/ determines both the duration of time that each spoke corresponds to, and how often the timeout
--   thread wakes. For example, with a resolution of __@1s@__, a timer that expires at __@2.5s@__ will not fire until
--   the timeout thread wakes at __@3s@__.
--
--     * A __larger resolution__ will result in __more insert contention__ at each spoke, __less accurate__ timers, and
--       will require __fewer wakeups__ by the timeout thread.
--
--     * A __smaller resolution__ will result in __less insert contention__ at each spoke, __more accurate__ timers, and
--       will require __more wakeups__ by the timeout thread.
--
-- * The timeout thread has some important properties:
--
--     * There is only one, and it fires expired timers synchronously. If your timer actions execute quicky, 'register'
--       them directly. Otherwise, consider registering an action that enqueues the /real/ action to be performed on a
--       job queue.
--
--     * Synchronous exceptions thrown by enqueued @IO@ actions will bring the thread down, and the exception will be
--       propagated to the thread that created the timer wheel. If you want to catch exceptions and log them, for
--       example, you will have to bake this into the registered actions yourself.
--
-- As an example, below is a depiction of a timer wheel with @6@ timers inserted across @8@ spokes, and a resolution of
-- @.1s@. It depicts a cursor at @.3s@, which indicates where the timeout thread currently is.
--
-- @
--  0       .1      .2      .3      .4      .5      .6      .7
-- ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
-- │       │ A⁰    │       │ B¹ C⁰ │ D⁰    │       │       │ E² F⁰ │
-- └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
--                           ↑
-- @
--
-- After @.1s@, the timeout thread will advance to the next spoke and process all of the timers it passed over. In
-- this case, __C__ will fire, and __B__ will be put back with its count decremented to @0@. This is how the timer wheel
-- can schedule a timer to fire arbitrarily far in the future: its count is simply the number of times its delay wraps
-- the entire duration of the timer wheel.
--
-- @
--  0       .1      .2      .3      .4      .5      .6      .7
-- ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
-- │       │ A⁰    │       │ B⁰    │ D⁰    │       │       │ E² F⁰ │
-- └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
--                                   ↑
-- @
data TimerWheel = TimerWheel
  { buckets :: {-# UNPACK #-} !(MutableArray RealWorld TimerBucket),
    resolution :: {-# UNPACK #-} !Nanoseconds,
    numTimers :: {-# UNPACK #-} !Counter,
    -- A counter to generate unique ints that identify registered actions, so they can be canceled.
    timerIdSupply :: {-# UNPACK #-} !Counter
  }

-- | Timer wheel config.
--
-- * @spokes@ must be ∈ @[1, maxBound]@, and is set to @1024@ if invalid.
-- * @resolution@ must be ∈ @(0, ∞]@, and is set to @1@ if invalid.
data Config = Config
  { -- | Spoke count
    spokes :: {-# UNPACK #-} !Int,
    -- | Resolution
    resolution :: {-# UNPACK #-} !Seconds
  }
  deriving stock (Generic, Show)

-- | Create a timer wheel in a scope.
create ::
  -- | ​
  Ki.Scope ->
  -- | ​
  Config ->
  -- | ​
  IO TimerWheel
create scope (Config spokes0 resolution0) = do
  buckets <- Array.newArray spokes TimestampMap.empty
  numTimers <- newCounter
  timerIdSupply <- newCounter
  Ki.fork_ scope (runTimerReaperThread buckets numTimers resolution)
  pure TimerWheel {buckets, numTimers, resolution, timerIdSupply}
  where
    spokes = if spokes0 <= 0 then 1024 else spokes0
    resolution = Nanoseconds.fromNonNegativeSeconds (if resolution0 <= 0 then 1 else resolution0)

-- | Perform an action with a timer wheel.
with ::
  -- | ​
  Config ->
  -- | ​
  (TimerWheel -> IO a) ->
  -- | ​
  IO a
with config action =
  Ki.scoped \scope -> do
    wheel <- create scope config
    action wheel

-- | @register wheel delay action@ registers an action __@action@__ in timer wheel __@wheel@__ to fire after __@delay@__
-- seconds.
--
-- Returns an action that, when called, attempts to cancel the timer, and returns whether or not it was successful
-- (@False@ means the timer has already fired, or has already been cancelled).
register ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before the action is fired
  Seconds ->
  -- | The action to fire
  IO () ->
  -- | An action that attempts to cancel the timer
  IO (IO Bool)
register wheel delay =
  registerImpl wheel (Nanoseconds.fromSeconds delay)

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before the action is fired
  Seconds ->
  -- | The action to fire
  IO () ->
  IO ()
register_ wheel delay action = do
  _ <- register wheel delay action
  pure ()

registerImpl :: TimerWheel -> Nanoseconds -> IO () -> IO (IO Bool)
registerImpl TimerWheel {buckets, numTimers, resolution, timerIdSupply} delay action = do
  now <- Timestamp.now
  incrCounter_ numTimers
  timerId <- incrCounter timerIdSupply
  let timestamp = now `Timestamp.plus` delay
  let index = timestampToIndex buckets resolution timestamp
  atomicModifyArray buckets index (timerBucketInsert timestamp (Timer timerId action))
  pure (atomicMaybeModifyArray buckets index (timerBucketDelete timestamp timerId))

-- | @recurring wheel action delay@ registers an action __@action@__ in timer wheel __@wheel@__ to fire every
-- __@delay@__ seconds (but no more often than __@wheel@__'s /resolution/).
--
-- Returns an action that, when called, cancels the recurring timer.
--
-- Note: it is possible (though very unlikely) for a canceled timer to fire one time after its cancelation.
recurring ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before each action is fired
  Seconds ->
  -- | The action to fire repeatedly
  IO () ->
  -- | An action that cancels the recurring timer
  IO (IO ())
recurring wheel (Nanoseconds.fromSeconds -> delay) action = do
  -- Hold the cancel action for the recurring timer in a mutable cell. Each time the timer is registered, this cancel
  -- action is updated accordingly to the latest.
  tryCancelRef <- newIORef undefined

  -- The action for a recurring timer first registers the next firing of the action, then updates the cancel action,
  -- then performs the action.
  let doAction :: IO ()
      doAction = do
        tryCancel <- reregister wheel delay doAction
        writeIORef tryCancelRef tryCancel
        -- At this moment, we've written the cancel action for the *next* recurring timer, but haven't executed *this*
        -- action. That means it's possible for the user to cancel the recurring timer, *then* observe this one last
        -- action. That seems fine, though - our semantics of "cancelation" are just "will be canceled eventually" (and,
        -- more precisely, happen to be "will occur at most one more time, but usually zero").
        action

  -- Register the first occurrence of the timer (which will re-register itself when it fires, and on and on).
  tryCancel0 <- registerImpl wheel delay doAction
  writeIORef tryCancelRef tryCancel0

  -- Track whether or not this recurring timer has been canceled, so we don't enter an infinite loop if the user
  -- accidentally calls cancel more than once.
  canceledRef <- newIORef False

  -- Return an action that attempt to cancel the timer over and over until success. It's very unlikely that any given
  -- attempt to cancel doesn't succeed, but it's possible with this thread interleaving:
  --
  --   1. User thread reads `tryCancel` from `tryCancelRef`.
  --   2. Reaper thread performs `doAction` to completion, which registers the next occurrence and performs the action.
  --   3. User thread calls `tryCancel`, observing that the associated action was already performed.
  let cancel = do
        readIORef canceledRef >>= \case
          True -> pure ()
          False -> do
            tryCancel <- readIORef tryCancelRef
            tryCancel >>= \case
              True -> writeIORef canceledRef True
              False -> cancel
  pure cancel

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
  TimerWheel ->
  -- | The delay before each action is fired
  Seconds ->
  -- | The action to fire repeatedly
  IO () ->
  IO ()
recurring_ wheel (Nanoseconds.fromSeconds -> delay) action = do
  _ <- registerImpl wheel delay doAction
  pure ()
  where
    doAction :: IO ()
    doAction = do
      _ <- reregister wheel delay doAction
      action

-- Re-register one bucket early, to account for the fact that timers are
-- expired at the *end* of a bucket.
--
-- +---+---+---+---+
-- { A |   |   |   }
-- +---+---+---+---+
--      |
--      The reaper thread fires 'A' approximately here, so if it's meant
--      to be repeated every two buckets, and we just re-register it at
--      this time, three buckets will pass before it's run again. So, we
--      act as if it's still "one bucket ago" at the moment we re-register
--      it.
reregister :: TimerWheel -> Nanoseconds -> IO () -> IO (IO Bool)
reregister wheel@TimerWheel {resolution} delay =
  registerImpl
    wheel
    if resolution > delay
      then Nanoseconds 0
      else delay `Nanoseconds.unsafeMinus` resolution

-- | Get the number of timers in a timer wheel.
--
-- /O(1)/.
count :: TimerWheel -> IO Int
count TimerWheel {numTimers} =
  readCounter numTimers

-- `timestampToIndex buckets resolution timestamp` figures out which index `timestamp` corresponds to in `buckets`,
-- where each bucket corresponds to `resolution` nanoseconds.
--
-- For example, consider a three-element `buckets` with resolution `1000000000`.
--
--   +--------------------------------------+
--   | 1000000000 | 1000000000 | 1000000000 |
--   +--------------------------------------+
--
-- Some timestamp like `1053298012387` gets binned to one of the three indices 0, 1, or 2, with quick and easy maffs:
--
--   1. Figure out which index the timestamp corresponds to, if there were infinitely many:
--
--        1053298012387 `div` 1000000000 = 1053
--
--   2. Wrap around per the actual length of the array:
--
--        1053 `rem` 3 = 0
timestampToIndex :: MutableArray RealWorld bucket -> Nanoseconds -> Timestamp -> Int
timestampToIndex buckets resolution timestamp =
  -- This downcast is safe because there are at most `maxBound :: Int` buckets (not that anyone would ever have that
  -- many...)
  fromIntegral @Word64 @Int
    (Timestamp.epoch resolution timestamp `rem` fromIntegral @Int @Word64 (Array.sizeofMutableArray buckets))

------------------------------------------------------------------------------------------------------------------------
-- Timer bucket operations

type TimerBucket =
  TimestampMap Timers

timerBucketDelete :: Timestamp -> TimerId -> TimerBucket -> Maybe TimerBucket
timerBucketDelete timestamp timerId bucket =
  case TimestampMap.lookup timestamp bucket of
    Nothing -> Nothing
    Just (Timers1 (Timer timerId1 _))
      | timerId == timerId1 -> Just $! TimestampMap.delete timestamp bucket
      | otherwise -> Nothing
    Just (TimersN timers0) ->
      case timersDelete timerId timers0 of
        Nothing -> Nothing
        Just timers1 ->
          let timers2 =
                case timers1 of
                  [timer] -> Timers1 timer
                  _ -> TimersN timers1
           in Just $! TimestampMap.insert timestamp timers2 bucket

timerBucketInsert :: Timestamp -> Timer -> TimerBucket -> TimerBucket
timerBucketInsert timestamp timer =
  TimestampMap.upsert timestamp (Timers1 timer) \case
    Timers1 old -> TimersN [timer, old]
    TimersN old -> TimersN (timer : old)

-- Fire every timer in a bucket, in order
timerBucketFire :: Counter -> TimerBucket -> IO ()
timerBucketFire numTimers =
  TimestampMap.foreach \_ -> \case
    Timers1 timer -> timerActionThenDecr timer
    TimersN timers -> foldr f (pure ()) timers
  where
    f :: Timer -> IO () -> IO ()
    f timer acc = do
      acc
      timerActionThenDecr timer

    timerActionThenDecr :: Timer -> IO ()
    timerActionThenDecr timer = do
      timerAction timer
      decrCounter_ numTimers

data Timers
  = Timers1 {-# UNPACK #-} !Timer
  | -- 2+ timers, stored in the reverse order that they were enqueued (so the last should fire first)
    TimersN ![Timer]

timersDelete :: TimerId -> [Timer] -> Maybe [Timer]
timersDelete timerId =
  go
  where
    go :: [Timer] -> Maybe [Timer]
    go = \case
      [] -> Nothing
      Timer timerId1 _ : timers | timerId == timerId1 -> Just timers
      timer : timers -> (timer :) <$> go timers

data Timer
  = Timer
      !TimerId
      !(IO ())

timerAction :: Timer -> IO ()
timerAction = \case
  Timer _ action -> action

type TimerId =
  Int

------------------------------------------------------------------------------------------------------------------------
-- Atomic operations on arrays

atomicModifyArray :: forall a. MutableArray RealWorld a -> Int -> (a -> a) -> IO ()
atomicModifyArray array index f = do
  ticket0 <- Atomics.readArrayElem array index
  loop ticket0
  where
    loop :: Atomics.Ticket a -> IO ()
    loop ticket = do
      (success, ticket1) <- Atomics.casArrayElem array index ticket (f (Atomics.peekTicket ticket))
      if success then pure () else loop ticket1

atomicMaybeModifyArray :: forall a. MutableArray RealWorld a -> Int -> (a -> Maybe a) -> IO Bool
atomicMaybeModifyArray buckets index doDelete = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop :: Atomics.Ticket a -> IO Bool
    loop ticket =
      case doDelete (Atomics.peekTicket ticket) of
        Nothing -> pure False
        Just bucket -> do
          (success, ticket1) <- Atomics.casArrayElem buckets index ticket bucket
          if success then pure True else loop ticket1

atomicExtractExpiredTimersFromBucket :: MutableArray RealWorld TimerBucket -> Int -> Timestamp -> IO TimerBucket
atomicExtractExpiredTimersFromBucket buckets index now = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop :: Atomics.Ticket TimerBucket -> IO TimerBucket
    loop ticket = do
      let Pair expired bucket1 = TimestampMap.splitL now (Atomics.peekTicket ticket)
      if TimestampMap.null expired
        then pure TimestampMap.empty
        else do
          (success, ticket1) <- Atomics.casArrayElem buckets index ticket bucket1
          if success then pure expired else loop ticket1

------------------------------------------------------------------------------------------------------------------------
-- Timer reaper thread

runTimerReaperThread :: MutableArray RealWorld TimerBucket -> Counter -> Nanoseconds -> IO void
runTimerReaperThread buckets numTimers resolution = do
  -- Sleep until the very first bucket of timers expires
  now <- Timestamp.now
  let remainingBucketNanos = resolution `Nanoseconds.unsafeMinus` (now `Timestamp.intoEpoch` resolution)
  Nanoseconds.sleep remainingBucketNanos

  loop (now `Timestamp.plus` remainingBucketNanos) (timestampToIndex buckets resolution now)
  where
    --   +----+----+----+----+----+----+
    --   |    |    |    |    |    |    |
    --   +----+----+----+----+----+----+
    --     ^  ^    ^
    --     |  |    nextTime = 7418238000
    --     |  |
    --     |  thisTime = 7418237000
    --     |
    --     index = 0
    --
    -- `loop thisTime index` does this in a loop:
    --
    --   1. Expires all timers at `index`, per `thisTime` (which may be arbitrarily behind reality)
    --   2. Sleeps until `nextTime`
    --   3. Loops with `thisTime = nextTime`, `index = index + 1`
    --
    -- Under "normal" conditions, wherein the timer thread can keep up with the timer actions, we'll be in a loop like
    -- this (using small numbers for monotonic timestamps, for readability)
    --
    --   1. Enter loop with `thisTime = 1000`, `index = 4`, and the actual timestamp is something like `now = 1004`,
    --      later than `thisTime` due to thread scheduling and such, but not by much.
    --   2. Expire all timers at `index = 4` whose expiry is <= 1000 (`thisTime`)
    --   3. Get the current time `now = 1150`
    --   4. Sleep for `2000 - 1150 = 850`
    --   5. Re-enter the loop with `thisTime = 2000`, `index = 5`
    --
    -- But things still work even if the reaper thread gets hopelessly behind schedule:
    --
    --   1. Enter loop with `thisTime = 1000`, `index = 4`, and the actual timestamp is something like `now = 3050`.
    --   2. Expire all timers at `index = 4` whose expiry is <= 1000 (`thisTime`)
    --   3. Re-enter the loop with `thisTime = 2000`, `index = 5`
    --
    -- Critically, we have not accidentally skipped ahead to the actual index that corresponds to `now`; rather, we
    -- always advance bucket-by-bucket no matter what the actual current timestamp is.
    loop :: Timestamp -> Int -> IO void
    loop !thisTime !index = do
      let !nextTime = thisTime `Timestamp.plus` resolution
      expired <- atomicExtractExpiredTimersFromBucket buckets index thisTime
      timerBucketFire numTimers expired
      now <- Timestamp.now
      when (nextTime > now) (Nanoseconds.sleep (nextTime `Timestamp.unsafeMinus` now))
      loop nextTime ((index + 1) `rem` Array.sizeofMutableArray buckets)
