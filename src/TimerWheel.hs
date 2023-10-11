-- | This module is intended to be imported qualified:
--
-- > import TimerWheel (TimerWheel)
-- > import TimerWheel qualified
module TimerWheel
  ( -- * Timer wheel
    TimerWheel,

    -- * Timer wheel configuration
    Config (..),
    Seconds,

    -- * Timer
    Timer,

    -- * Constructing a timer wheel
    create,
    with,

    -- * Querying a timer wheel
    count,

    -- * Registering timers in a timer wheel
    register,
    register_,
    recurring,
    recurring_,

    -- * Canceling timers
    cancel,
  )
where

import Control.Exception (mask_)
import qualified Data.Atomics as Atomics
import Data.Functor (void)
import Data.Primitive.Array (MutableArray)
import qualified Data.Primitive.Array as Array
import GHC.Base (RealWorld)
import qualified Ki
import TimerWheel.Internal.Bucket (Bucket)
import qualified TimerWheel.Internal.Bucket as Bucket
import TimerWheel.Internal.Counter (Counter, decrCounter_, incrCounter, incrCounter_, newCounter, readCounter)
import TimerWheel.Internal.Nanoseconds (Nanoseconds (..))
import qualified TimerWheel.Internal.Nanoseconds as Nanoseconds
import TimerWheel.Internal.Prelude
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp

-- | A timer wheel is a vector-of-collections-of timers to fire. Timers may be one-shot or recurring, and may be
-- scheduled arbitrarily far in the future.
--
-- A timer wheel is configured with a /spoke count/ and /resolution/:
--
-- * The /spoke count/ determines the size of the timer vector.
--
--     A __larger spoke count__ will require __more memory__, but will result in __less insert contention__.
--
-- * The /resolution/ determines the duration of time that each spoke corresponds to, and thus how often timers are
--   checked for expiry.
--
--     For example, in a timer wheel with a /resolution/ of __@1 second@__, a timer that is scheduled to fire at
--     __@8.4 o'clock@__ will end up firing around __@9.0 o'clock@__ instead (that is, on the
--     __@1 second@__-boundary).
--
--     A __larger resolution__ will result in __more insert contention__ and __less accurate timers__, but will require
--     __fewer wakeups__ by the timeout thread.
--
-- The timeout thread has some important properties:
--
--     * There is only one, and it fires expired timers synchronously. If your timer actions execute quicky, you can
--       'register' them directly. Otherwise, consider registering an action that enqueues the real action to be
--       performed on a job queue.
--
--     * A synchronous exception thrown by a registered timer will bring the timeout thread down, and the exception will
--       be propagated to the thread that created the timer wheel. If you want to log and ignore exceptions, for example,
--       you will have to bake this into the registered actions yourself.
--
-- __API summary__
--
-- +----------+---------+----------------+
-- | Create   | Query   | Modify         |
-- +==========+=========+================+
-- | 'create' | 'count' | 'register'     |
-- +----------+---------+----------------+
-- | 'with'   |         | 'register_'    |
-- +----------+         +----------------+
-- |          |         | 'recurring'    |
-- |          |         +----------------+
-- |          |         | 'recurring_'   |
-- +----------+---------+----------------+
data TimerWheel = TimerWheel
  { buckets :: {-# UNPACK #-} !(MutableArray RealWorld (Bucket Timer0)),
    resolution :: {-# UNPACK #-} !Nanoseconds,
    numTimers :: {-# UNPACK #-} !Counter,
    -- A counter to generate unique ints that identify registered actions, so they can be canceled.
    timerIdSupply :: {-# UNPACK #-} !Counter
  }

-- | A timer wheel config.
--
-- * @spokes@ must be ∈ @[1, maxBound]@, and is set to @1024@ if invalid.
-- * @resolution@ must be ∈ @(0, ∞]@, and is set to @1@ if invalid.
--
-- __API summary__
--
-- +----------+
-- | Create   |
-- +==========+
-- | 'Config' |
-- +----------+
data Config = Config
  { -- | Spoke count
    spokes :: {-# UNPACK #-} !Int,
    -- | Resolution
    resolution :: !Seconds
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
  buckets <- Array.newArray spokes Bucket.empty
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

-- | Get the number of timers in a timer wheel.
--
-- /O(1)/.
count :: TimerWheel -> IO Int
count TimerWheel {numTimers} =
  readCounter numTimers

-- | @register wheel delay action@ registers __@action@__ in __@wheel@__ to fire after __@delay@__ seconds.
--
-- When canceled, the timer returns whether or not the cancelation was successful; @False@ means the timer had either
-- already fired, or had already been canceled.
register ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before the action is fired
  Seconds ->
  -- | The action to fire
  IO () ->
  -- | The timer
  IO (Timer Bool)
register TimerWheel {buckets, numTimers, resolution, timerIdSupply} delay action = do
  now <- Timestamp.now
  let timestamp = now `Timestamp.plus` Nanoseconds.fromSeconds delay
  let index = timestampToIndex buckets resolution timestamp
  timerId <- incrCounter timerIdSupply
  mask_ do
    atomicModifyArray buckets index (Bucket.insert timerId timestamp (OneShot1 action))
    incrCounter_ numTimers
  coerce @(IO (IO Bool)) @(IO (Timer Bool)) do
    pure do
      mask_ do
        deleted <- atomicMaybeModifyArray buckets index (Bucket.deleteExpectingHit timerId)
        when deleted (decrCounter_ numTimers)
        pure deleted

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before the action is fired
  Seconds ->
  -- | The action to fire
  IO () ->
  IO ()
register_ wheel delay action =
  void (register wheel delay action)

-- | @recurring wheel action delay@ registers __@action@__ in __@wheel@__ to fire in __@delay@__ seconds, and every
-- __@delay@__ seconds thereafter.
recurring ::
  -- | The timer wheel
  TimerWheel ->
  -- | The delay before each action is fired
  Seconds ->
  -- | The action to fire repeatedly
  IO () ->
  -- | The timer
  IO (Timer ())
recurring TimerWheel {buckets, numTimers, resolution, timerIdSupply} (Nanoseconds.fromSeconds -> delay) action = do
  now <- Timestamp.now
  let timestamp = now `Timestamp.plus` delay
  let index = timestampToIndex buckets resolution timestamp
  timerId <- incrCounter timerIdSupply
  canceledRef <- newIORef False
  mask_ do
    atomicModifyArray buckets index (Bucket.insert timerId timestamp (Recurring1 action delay canceledRef))
    incrCounter_ numTimers
  coerce @(IO (IO ())) @(IO (Timer ())) do
    pure do
      mask_ do
        writeIORef canceledRef True
        decrCounter_ numTimers

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
  TimerWheel ->
  -- | The delay before each action is fired
  Seconds ->
  -- | The action to fire repeatedly
  IO () ->
  IO ()
recurring_ TimerWheel {buckets, numTimers, resolution, timerIdSupply} (Nanoseconds.fromSeconds -> delay) action = do
  now <- Timestamp.now
  let timestamp = now `Timestamp.plus` delay
  let index = timestampToIndex buckets resolution timestamp
  timerId <- incrCounter timerIdSupply
  mask_ do
    atomicModifyArray buckets index (Bucket.insert timerId timestamp (Recurring1_ action delay))
    incrCounter_ numTimers

-- | A registered timer, parameterized by the result of attempting to cancel it:
--
--     * A one-shot timer may only be canceled if it has not already fired.
--     * A recurring timer can always be canceled.
--
-- __API summary__
--
-- +-------------+----------+
-- | Create      | Modify   |
-- +=============+==========+
-- | 'register'  | 'cancel' |
-- +-------------+----------+
-- | 'recurring' |          |
-- +-------------+----------+
newtype Timer a
  = Timer (IO a)

-- | Cancel a timer.
cancel :: Timer a -> IO a
cancel =
  coerce

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

data Timer0
  = OneShot1 !(IO ())
  | Recurring1 !(IO ()) !Nanoseconds !(IORef Bool)
  | Recurring1_ !(IO ()) !Nanoseconds

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

atomicExtractExpiredTimersFromBucket :: MutableArray RealWorld (Bucket Timer0) -> Int -> Timestamp -> IO (Bucket Timer0)
atomicExtractExpiredTimersFromBucket buckets index now = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop :: Atomics.Ticket (Bucket Timer0) -> IO (Bucket Timer0)
    loop ticket = do
      let Bucket.Pair expired bucket1 = Bucket.partition now (Atomics.peekTicket ticket)
      if Bucket.isEmpty expired
        then pure Bucket.empty
        else do
          (success, ticket1) <- Atomics.casArrayElem buckets index ticket bucket1
          if success then pure expired else loop ticket1

------------------------------------------------------------------------------------------------------------------------
-- Timer reaper thread
--
-- The main loop is rather simple, but the code is somewhat fiddly. In brief, the reaper thread wakes up to fire all of
-- the expired timers in bucket N, then sleeps, then wakes up to fire all of the expired timers in bucket N+1, then
-- sleeps, and so on, forever.
--
-- It wakes up on the "bucket boundaries", that is,
--
--   +------+------+------+------+------+------+------+------+------+------+
--   |      |      |      |      |      |      |      |      |      |      |
--   |      |      |      |      |      |      |      |      |      |      |
--   +------+------+------+------+------+------+------+------+------+------+
--                           ^   ^
--                           |   we wake up around here
--                           |
--                           to fire all of the expired timers stored here
--
-- It's entirely possible the reaper thread gets hopelessly behind, that is, it's taken so long to expire all of the
-- timers in previous buckets that we're behind schedule an entire bucket or more. That might look like this:
--
--   +------+------+------+------+------+------+------+------+------+------+
--   |      |      |      |      |      |      |      |      |      |      |
--   |      |      |      |      |      |      |      |      |      |      |
--   +------+------+------+------+------+------+------+------+------+------+
--                           ^                    ^
--                           |                    we are very behind, and enter the loop around here
--                           |
--                           yet we nonetheless fire all of the expired timers stored here, as if we were on time
--
-- That's accomplished simplly by maintaining in the loop state the "ideal" time that we wake up, ignoring reality. We
-- only ultimately check the *actual* current time when determining how long to *sleep* after expiring all of the timers
-- in the current bucket. If we're behind schedule, we won't sleep at all.
--
--   +------+------+------+------+------+------+------+------+------+------+
--   |      |      |      |      |      |      |      |      |      |      |
--   |      |      |      |      |      |      |      |      |      |      |
--   +------+------+------+------+------+------+------+------+------+------+
--                           ^   ^                  ^
--                           |   |                  |
--                           |   we enter the loop with this "ideal" time
--                           |                      |
--                           to fire timers in here |
--                                                  |
--                                                  not caring how far ahead the actual current time is
--
-- On to expiring timers: a "bucket" of timers is stored at each array index, which can be partitioned into "expired"
-- (meant to fire at or before the ideal time) and "not expired" (to expire on a subsequent wrap around the bucket
-- array).
--
--   +-----------------------+
--   |           /           |
--   | expired  /            |
--   |         / not expired |
--   |        /              |
--   +-----------------------+
--
-- The reaper thread simply atomically partitions the bucket, keeping the expired collection for itself, and putting the
-- not-expired collection back in the array.
--
-- Next, the timers are carefully fired one-by-one, in timestamp order. It's possible that two or more timers are
-- scheduled to expire concurrently (i.e. on the same nanosecond); that's fine: we fire them in the order they were
-- scheduled.
--
-- Let's say this is our set of timers to fire.
--
--    Ideal time         Timers to fire
--   +--------------+   +-----------------------------+
--   | 700          |   | Expiry | Type               |
--   +--------------+   +--------+--------------------+
--                      | 630    | One-shot           |
--    Next ideal time   | 643    | Recurring every 10 |
--   +--------------+   | 643    | One-shot           |
--   | 800          |   | 689    | Recurring every 80 |
--   +--------------+   +--------+--------------------+
--
-- Expiring a one-shot timer is simple: call the IO action and move on.
--
-- Expiring a recurring timer is less simple (but still simple): call the IO action, then schedule the next occurrence.
-- There are two possibilities.
--
--   1. The next occurrence is *at or before* the ideal time, which means it ought to fire along with the other timers
--      in the queue, right now. So, insert it into the collection of timers to fire.
--
--   2. The next occurrence is *after* the ideal time, so enqueue it in the array of buckets wherever it belongs.
--
-- After all expired timers are fired, the reaper thread has one last decision to make: how long should we sleep? We
-- get the current timestamp, and if it's still before the next ideal time (i.e. the current ideal time plus the wheel
-- resolution), then we sleep for the difference.
--
-- If the actual time is at or after the next ideal time, that's kind of bad - it means the reaper thread is behind
-- schedule. The user's enqueued actions have taken too long, or their wheel resolution is too short. Anyway, it's not
-- our problem, our behavior doesn't change per whether we are behind schedule or not.
runTimerReaperThread :: MutableArray RealWorld (Bucket Timer0) -> Counter -> Nanoseconds -> IO void
runTimerReaperThread buckets numTimers resolution = do
  -- Sleep until the very first bucket of timers expires
  --
  --     resolution                         = 100
  --     now                                = 184070
  --     progress   = now % resolution      = 70
  --     remaining  = resolution - progress = 30
  --     idealTime  = now + remaining       = 184100
  --
  --   +-------------------------+----------------+---------
  --   | progress = 70           | remaining = 30 |
  --   +-------------------------+----------------+
  --   | resolution = 100                         |
  --   +------------------------------------------+---------
  --                             ^                ^
  --                             now              idealTime
  now <- Timestamp.now
  let progress = now `Timestamp.intoEpoch` resolution
  let remaining = resolution `Nanoseconds.unsafeMinus` progress
  Nanoseconds.sleep remaining
  -- Enter the Loop™
  let idealTime = now `Timestamp.plus` remaining
  theLoop idealTime (timestampToIndex buckets resolution now)
  where
    -- `index` could be derived from `thisTime`, but it's cheaper to just store it separately and bump by 1 as we go
    theLoop :: Timestamp -> Int -> IO void
    theLoop !idealTime !index = do
      expired2 <- atomicExtractExpiredTimersFromBucket buckets index idealTime
      fireTimerBucket expired2
      let !nextIdealTime = idealTime `Timestamp.plus` resolution
      now <- Timestamp.now
      when (nextIdealTime > now) (Nanoseconds.sleep (nextIdealTime `Timestamp.unsafeMinus` now))
      theLoop nextIdealTime ((index + 1) `rem` Array.sizeofMutableArray buckets)
      where
        fireTimerBucket :: Bucket Timer0 -> IO ()
        fireTimerBucket bucket0 =
          case Bucket.pop bucket0 of
            Bucket.PopNada -> pure ()
            Bucket.PopAlgo timerId timestamp timer bucket1 -> do
              expired2 <- fireTimer bucket1 timerId timestamp timer
              fireTimerBucket expired2

        fireTimer :: Bucket Timer0 -> TimerId -> Timestamp -> Timer0 -> IO (Bucket Timer0)
        fireTimer bucket timerId timestamp timer =
          case timer of
            OneShot1 action -> do
              action
              decrCounter_ numTimers
              pure bucket
            Recurring1 action delay canceledRef ->
              readIORef canceledRef >>= \case
                True -> pure bucket
                False -> do
                  action
                  scheduleNextOccurrence (timestamp `Timestamp.plus` delay)
            Recurring1_ action delay -> do
              action
              scheduleNextOccurrence (timestamp `Timestamp.plus` delay)
          where
            scheduleNextOccurrence :: Timestamp -> IO (Bucket Timer0)
            scheduleNextOccurrence nextOccurrence =
              if nextOccurrence < idealTime
                then pure $! insertNextOccurrence bucket
                else do
                  atomicModifyArray
                    buckets
                    (timestampToIndex buckets resolution nextOccurrence)
                    insertNextOccurrence
                  pure bucket
              where
                insertNextOccurrence :: Bucket Timer0 -> Bucket Timer0
                insertNextOccurrence =
                  Bucket.insert timerId nextOccurrence timer
