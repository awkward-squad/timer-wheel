{-# LANGUAGE RecursiveDo #-}

-- | A simple, hashed timer wheel.
module TimerWheel
  ( -- * Timer wheel
    TimerWheel,
    create,
    with,
    Config (..),
    register,
    register_,
    recurring,
    recurring_,
  )
where

import Data.Bool (bool)
import Data.Fixed (E6, Fixed)
import Data.Function (fix)
import Data.IORef (newIORef, readIORef, writeIORef)
import qualified Ki
import TimerWheel.Internal.Config (Config (..))
import TimerWheel.Internal.Counter (Counter, incrCounter, newCounter)
import TimerWheel.Internal.Micros (Micros (Micros))
import qualified TimerWheel.Internal.Micros as Micros
import TimerWheel.Internal.Timers (Timers)
import qualified TimerWheel.Internal.Timers as Timers

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
  { -- A counter, to generate unique ints that identify registered actions.
    counter :: {-# UNPACK #-} !Counter,
    -- The array of collections of timers.
    timers :: {-# UNPACK #-} !Timers
  }

-- | Create a timer wheel in a scope.
create :: Ki.Scope -> Config -> IO TimerWheel
create scope Config {spokes, resolution} = do
  counter <- newCounter
  timers <-
    Timers.create
      (if spokes <= 0 then 1024 else spokes)
      (Micros.fromFixed (if resolution <= 0 then 1 else resolution))
  Ki.fork_ scope (Timers.reap timers)
  pure TimerWheel {counter, timers}

-- | Perform an action with a timer wheel.
--
-- /Throws./
--
--   * Throws the exception the given action throws, if any
--   * Throws the exception the timer wheel thread throws, if any
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
  TimerWheel ->
  -- | Delay, in seconds
  Fixed E6 ->
  -- | Action
  IO () ->
  IO (IO Bool)
register wheel delay =
  registerImpl wheel (Micros.fromSeconds (max 0 delay))

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ ::
  TimerWheel ->
  -- | Delay, in seconds
  Fixed E6 ->
  -- | Action
  IO () ->
  IO ()
register_ wheel delay action = do
  _ <- register wheel delay action
  pure ()

registerImpl :: TimerWheel -> Micros -> IO () -> IO (IO Bool)
registerImpl TimerWheel {counter, timers} delay action = do
  key <- incrCounter counter
  Timers.insert timers key delay action

-- | @recurring wheel action delay@ registers an action __@action@__ in timer wheel __@wheel@__ to fire every
-- __@delay@__ seconds.
--
-- Returns an action that, when called, cancels the recurring timer.
recurring ::
  TimerWheel ->
  -- | Delay, in seconds
  Fixed E6 ->
  -- | Action
  IO () ->
  IO (IO ())
recurring wheel (Micros.fromSeconds -> delay) action = mdo
  let doAction :: IO ()
      doAction = do
        cancel <- reregister wheel delay doAction
        writeIORef cancelRef cancel
        action
  cancel0 <- registerImpl wheel delay doAction
  cancelRef <- newIORef cancel0
  pure do
    untilTrue do
      cancel <- readIORef cancelRef
      cancel

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
  TimerWheel ->
  -- | Delay, in seconds
  Fixed E6 ->
  -- | Action
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
reregister wheel@TimerWheel {timers} delay =
  registerImpl wheel (if reso > delay then Micros 0 else delay `Micros.minus` reso)
  where
    reso :: Micros
    reso =
      Timers.resolution timers

-- Repeat an IO action until it returns 'True'.
untilTrue :: IO Bool -> IO ()
untilTrue m =
  fix \again ->
    m >>= bool again (pure ())
