{-# LANGUAGE RecursiveDo #-}

-- | A simple, hashed timer wheel.
module TimerWheel
  ( -- * Timer wheel
    TimerWheel,
    Config (..),
    Seconds,

    -- ** Constructing a timer wheel
    create,
    with,

    -- ** Registering timers
    register,
    register_,
    recurring,
    recurring_,

    -- ** Querying a timer wheel
    count,
  )
where

import qualified Data.Atomics as Atomics
import Data.Foldable (for_)
import Data.Primitive.Array (MutableArray)
import qualified Data.Primitive.Array as Array
import GHC.Base (RealWorld)
import qualified Ki
import TimerWheel.Internal.Counter (Counter, decrCounter_, incrCounter, incrCounter_, newCounter, readCounter)
import TimerWheel.Internal.Entries (Entries)
import qualified TimerWheel.Internal.Entries as Entries
import TimerWheel.Internal.Micros (Micros (..))
import qualified TimerWheel.Internal.Micros as Micros
import TimerWheel.Internal.Prelude
import TimerWheel.Internal.Timestamp (Timestamp)
import qualified TimerWheel.Internal.Timestamp as Timestamp

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
  { buckets :: {-# UNPACK #-} !(MutableArray RealWorld Entries),
    resolution :: {-# UNPACK #-} !Micros,
    numTimers :: {-# UNPACK #-} !Counter,
    -- A counter to generate unique ints that identify registered actions, so they can be canceled.
    timerIdSupply :: {-# UNPACK #-} !Counter,
    -- The total number of microseconds that one trip 'round the wheel represents. This value is needed whenever a new
    -- timer is inserted, so we cache it, though this doesn't really show up on any benchmark we've put together heh
    totalMicros :: {-# UNPACK #-} !Micros
  }

-- Internal type alias for readability
type TimerId = Int

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
create :: Ki.Scope -> Config -> IO TimerWheel
create scope (Config spokes0 resolution0) = do
  buckets <- Array.newArray spokes Entries.empty
  numTimers <- newCounter
  timerIdSupply <- newCounter
  Ki.fork_ scope (runTimerReaperThread buckets numTimers resolution)
  pure TimerWheel {buckets, numTimers, resolution, timerIdSupply, totalMicros}
  where
    spokes = if spokes0 <= 0 then 1024 else spokes0
    resolution = Micros.fromNonNegativeSeconds (if resolution0 <= 0 then 1 else resolution0)
    totalMicros = Micros.scale spokes resolution

-- | Perform an action with a timer wheel.
with :: Config -> (TimerWheel -> IO a) -> IO a
with config action =
  Ki.scoped \scope -> do
    wheel <- create scope config
    action wheel

-- | @register wheel delay action@ registers an action __@action@__ in timer wheel __@wheel@__ to fire after __@delay@__
-- seconds.
--
-- Returns an action that, when called, attempts to cancel the timer, and returns whether or not it was successful
-- (@False@ means the timer has already fired, or was already cancelled).
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
  registerImpl wheel (Micros.fromSeconds delay)

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

registerImpl :: TimerWheel -> Micros -> IO () -> IO (IO Bool)
registerImpl TimerWheel {buckets, numTimers, resolution, timerIdSupply, totalMicros} delay action = do
  now <- Timestamp.now
  incrCounter_ numTimers
  timerId <- incrCounter timerIdSupply
  let index = timestampToIndex buckets resolution (now `Timestamp.plus` delay)
  atomicInsertIntoBucket buckets totalMicros index timerId delay action
  pure (atomicDeleteFromBucket buckets index timerId)

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
recurring wheel (Micros.fromSeconds -> delay) action = do
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

  -- Return an action that attempt to cancel the timer over and over until success. It's very unlikely that any given
  -- attempt to cancel doesn't succeed, but it's possible with this thread interleaving:
  --
  --   1. User thread reads `tryCancel` from `tryCancelRef`.
  --   2. Reaper thread performs `doAction` to completion, which registers the next occurrence and performs the action.
  --   3. User thread calls `tryCancel`, observing that the associated action was already performed.
  let cancel = do
        tryCancel <- readIORef tryCancelRef
        tryCancel >>= \case
          False -> cancel
          True -> pure ()
  pure cancel

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
  TimerWheel ->
  -- | The delay before each action is fired
  Seconds ->
  -- | The action to fire repeatedly
  IO () ->
  IO ()
recurring_ wheel (Micros.fromSeconds -> delay) action = do
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
reregister :: TimerWheel -> Micros -> IO () -> IO (IO Bool)
reregister wheel@TimerWheel {resolution} delay =
  registerImpl
    wheel
    if resolution > delay
      then Micros 0
      else delay `Micros.minus` resolution

-- | Get the approximate timer count in a timer wheel.
--
-- /O(1)/.
count :: TimerWheel -> IO Int
count TimerWheel {numTimers} =
  readCounter numTimers

timestampToIndex :: MutableArray RealWorld Entries -> Micros -> Timestamp -> Int
timestampToIndex buckets resolution timestamp =
  -- This downcast is safe because there are at most `maxBound :: Int` buckets (not that anyone would ever have that
  -- many...)
  fromIntegral @Word64 @Int
    (Timestamp.epoch resolution timestamp `rem` fromIntegral @Int @Word64 (Array.sizeofMutableArray buckets))

------------------------------------------------------------------------------------------------------------------------
-- Atomic operations on buckets

atomicInsertIntoBucket :: MutableArray RealWorld Entries -> Micros -> Int -> TimerId -> Micros -> IO () -> IO ()
atomicInsertIntoBucket buckets totalMicros index timerId delay action = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket = do
      (success, ticket1) <- Atomics.casArrayElem buckets index ticket (doInsert (Atomics.peekTicket ticket))
      if success then pure () else loop ticket1

    doInsert :: Entries -> Entries
    doInsert =
      Entries.insert timerId (unMicros (delay `Micros.div` totalMicros)) action

atomicDeleteFromBucket :: MutableArray RealWorld Entries -> Int -> TimerId -> IO Bool
atomicDeleteFromBucket buckets index timerId = do
  ticket0 <- Atomics.readArrayElem buckets index
  loop ticket0
  where
    loop ticket =
      case Entries.delete timerId (Atomics.peekTicket ticket) of
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

------------------------------------------------------------------------------------------------------------------------
-- Timer reaper thread

runTimerReaperThread :: MutableArray RealWorld Entries -> Counter -> Micros -> IO void
runTimerReaperThread buckets numTimers resolution = do
  now <- Timestamp.now
  let remainingBucketMicros = resolution `Micros.minus` (now `Timestamp.rem` resolution)
  Micros.sleep remainingBucketMicros
  loop
    (now `Timestamp.plus` remainingBucketMicros `Timestamp.plus` resolution)
    (timestampToIndex buckets resolution now)
  where
    loop :: Timestamp -> Int -> IO a
    loop !nextTime !index = do
      expired <- atomicExtractExpiredTimersFromBucket buckets index
      for_ expired \action -> do
        action
        decrCounter_ numTimers
      afterTime <- Timestamp.now
      when (afterTime < nextTime) (Micros.sleep (nextTime `Timestamp.minus` afterTime))
      loop (nextTime `Timestamp.plus` resolution) ((index + 1) `rem` Array.sizeofMutableArray buckets)
