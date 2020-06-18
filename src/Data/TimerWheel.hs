{-# LANGUAGE RecursiveDo #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}

module Data.TimerWheel
  ( -- * Timer wheel
    TimerWheel,
    with,
    Config (..),
    register,
    register_,
    recurring,
    recurring_,
    InvalidTimerWheelConfig (..),
    TimerWheelDied (..),
  )
where

import Control.Concurrent
import Control.Exception
import Control.Monad (join, void, when)
import Data.Fixed (E6, Fixed (MkFixed))
import Data.Functor (($>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import GHC.Generics (Generic)
import Micros (Micros (Micros))
import qualified Micros
import Numeric.Natural (Natural)
import Supply (Supply)
import qualified Supply
import Wheel (Wheel)
import qualified Wheel

-- | A 'TimerWheel' is a vector-of-collections-of timers to fire. It is configured with a /spoke count/ and
-- /resolution/. Timers may be scheduled arbitrarily far in the future. A timeout thread is spawned to step through the
-- timer wheel and fire expired timers at regular intervals.
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
--     * Synchronous exceptions thrown by enqueued @IO@ actions will bring the thread down, which will cause it to
--       asynchronously throw a 'TimerWheelDied' exception to the thread that 'create'd it. If you want to catch
--       exceptions and log them, for example, you will have to bake this into the registered actions yourself.
--
-- Below is a depiction of a timer wheel with @6@ timers inserted across @8@ spokes, and a resolution of @0.1s@.
--
-- @
--    0s   .1s   .2s   .3s   .4s   .5s   .6s   .7s   .8s
--    +-----+-----+-----+-----+-----+-----+-----+-----+
--    |     | A   |     | B,C | D   |     |     | E,F |
--    +-----+-----+-----+-----+-----+-----+-----+-----+
-- @
data TimerWheel = TimerWheel
  { -- | A supply of unique ints.
    wheelSupply :: !Supply,
    -- | The array of collections of timers.
    wheelWheel :: !Wheel,
    wheelThread :: !ThreadId
  }

data Config = Config
  { -- | Spoke count.
    spokes :: !Natural,
    -- | Resolution, in seconds.
    resolution :: !(Fixed E6)
  }
  deriving stock (Generic, Show)

-- | The timeout thread died.
newtype TimerWheelDied
  = TimerWheelDied SomeException
  deriving stock (Show)

instance Exception TimerWheelDied where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | The timer wheel config was invalid.
--
-- * @spokes@ must be positive, and less than @maxBound@ of @Int@.
-- * @resolution@ must be positive.
data InvalidTimerWheelConfig
  = InvalidTimerWheelConfig !Config
  deriving stock (Show)
  deriving anyclass (Exception)

-- | Perform an action with a timer wheel.
--
-- /Throws./ If the config is invalid, throws 'InvalidTimerWheelConfig'.
--
-- /Throws./ If the timeout thread dies, throws 'TimerWheelDied'.
with :: Config -> (TimerWheel -> IO a) -> IO a
with config action =
  uninterruptibleMask \restore -> do
    wheel <- create config
    let cleanup = destroy wheel
    result <- restore (action wheel) `onException` cleanup
    cleanup $> result

create :: Config -> IO TimerWheel
create config@(Config {spokes, resolution}) = do
  when (invalidConfig config) (throwIO (InvalidTimerWheelConfig config))
  wheel <- Wheel.create (fromIntegral spokes) (Micros.fromFixed resolution)
  supply <- Supply.new

  thread <- myThreadId
  reaperThread <-
    forkIOWithUnmask $ \unmask ->
      unmask (Wheel.reap wheel)
        `catch` \e ->
          case fromException e of
            Just ThreadKilled -> pure ()
            _ -> throwTo thread (TimerWheelDied e)

  pure
    TimerWheel
      { wheelSupply = supply,
        wheelWheel = wheel,
        wheelThread = reaperThread
      }

-- | Tear down a timer wheel by killing the timeout thread.
destroy :: TimerWheel -> IO ()
destroy wheel =
  killThread (wheelThread wheel)

invalidConfig :: Config -> Bool
invalidConfig Config {spokes, resolution} =
  or
    [ spokes == 0,
      spokes > fromIntegral (maxBound :: Int),
      resolution <= 0
    ]

-- | @register wheel action delay@ registers an action __@action@__ in timer wheel __@wheel@__ to fire after __@delay@__
-- seconds.
--
-- Returns an action that, when called, attempts to cancel the timer, and returns whether or not it was successful
-- (@False@ means the timer has already fired).
--
-- Subsequent calls to the cancel action have no effect, and continue to return whatever the first result was.
register ::
  -- |
  TimerWheel ->
  -- | Action
  IO () ->
  -- | Delay, in seconds
  Fixed E6 ->
  IO (IO Bool)
register wheel action (secondsToMicros -> delay) =
  _register wheel action delay

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ ::
  -- |
  TimerWheel ->
  -- | Action
  IO () ->
  -- | Delay, in seconds
  Fixed E6 ->
  IO ()
register_ wheel action delay =
  void (register wheel action delay)

_register :: TimerWheel -> IO () -> Micros -> IO (IO Bool)
_register wheel action delay = do
  key <- Supply.next (wheelSupply wheel)
  Wheel.insert (wheelWheel wheel) key action delay

_reregister :: TimerWheel -> IO () -> Micros -> IO (IO Bool)
_reregister wheel action delay =
  _register wheel action (if reso > delay then Micros 0 else delay `Micros.minus` reso)
  where
    reso :: Micros
    reso = Wheel.resolution (wheelWheel wheel)

-- | @recurring wheel action delay@ registers an action __@action@__ in timer wheel __@wheel@__ to fire every
-- __@delay@__ seconds, or every /resolution/ seconds, whichever is smaller.
--
-- Returns an action that, when called, cancels the recurring timer.
recurring ::
  TimerWheel ->
  -- | Action
  IO () ->
  -- | Delay, in seconds
  Fixed E6 ->
  IO (IO ())
recurring wheel action (secondsToMicros -> delay) = mdo
  let doAction :: IO ()
      doAction = do
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
        writeIORef cancelRef =<< _reregister wheel doAction delay
        action

  cancel :: IO Bool <-
    _register wheel doAction delay

  cancelRef :: IORef (IO Bool) <-
    newIORef cancel

  pure (untilTrue (join (readIORef cancelRef)))

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
  TimerWheel ->
  -- | Action
  IO () ->
  -- | Delay, in seconds
  Fixed E6 ->
  IO ()
recurring_ wheel action (secondsToMicros -> delay) =
  void (_register wheel doAction delay)
  where
    doAction :: IO ()
    doAction = do
      _ <- _reregister wheel doAction delay
      action

secondsToMicros :: Fixed E6 -> Micros
secondsToMicros (MkFixed micros) =
  Micros (fromIntegral (max 0 micros))

-- Repeat an IO action until it returns 'True'.
untilTrue :: IO Bool -> IO ()
untilTrue action =
  action >>= \case
    True -> pure ()
    False -> untilTrue action
