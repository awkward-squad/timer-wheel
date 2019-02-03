{-# OPTIONS_GHC -funbox-strict-fields #-}

module Data.TimerWheel
  ( -- * Timer wheel
    TimerWheel
  , create
  , destroy
  , Config(..)
  , register
  , register_
  , recurring
  , recurring_
  , InvalidTimerWheelConfig(..)
  , TimerWheelDied(..)
  ) where

import Supply (Supply)
import Wheel  (Wheel)

import qualified Supply
import qualified Wheel

import Control.Concurrent (ThreadId, forkIOWithUnmask, killThread, myThreadId,
                           throwTo)
import Control.Exception  (AsyncException(ThreadKilled),
                           Exception(fromException, toException), SomeException,
                           asyncExceptionFromException,
                           asyncExceptionToException, catch, throwIO)
import Control.Monad      (join, void, when)
import Data.Coerce        (coerce)
import Data.Fixed         (E6, Fixed(MkFixed))
import Data.IORef         (IORef, newIORef, readIORef, writeIORef)
import Data.Word          (Word64)
import GHC.Generics       (Generic)
import Numeric.Natural    (Natural)

-- | A 'TimerWheel' is a vector-of-collections-of timers to fire. It is
-- configured with a /spoke count/ and /resolution/. Timers may be scheduled
-- arbitrarily far in the future. A timeout thread is spawned to step through
-- the timer wheel and fire expired timers at regular intervals.
--
-- * The /spoke count/ determines the size of the timer vector.
--
--     * A __larger spoke count__ will result in __less insert contention__ at
--       each spoke and will require __more memory__ to store the timer wheel.
--
--     * A __smaller spoke count__ will result in __more insert contention__ at
--       each spoke and will require __less memory__ to store the timer wheel.
--
-- * The /resolution/ determines both the duration of time that each spoke
--   corresponds to, and how often the timeout thread wakes. For example, with a
--   resolution of __@1s@__, a timer that expires at __@2.5s@__ will not fire
--   until the timeout thread wakes at __@3s@__.
--
--     * A __larger resolution__ will result in __more insert contention__ at
--       each spoke, __less accurate__ timers, and will require
--       __fewer wakeups__ by the timeout thread.
--
--     * A __smaller resolution__ will result in __less insert contention__ at
--       each spoke, __more accurate__ timers, and will require __more wakeups__
--       by the timeout thread.
--
-- * The timeout thread has some important properties:
--
--     * There is only one, and it fires expired timers synchronously. If your
--       timer actions execute quicky, 'register' them directly. Otherwise,
--       consider registering an action that enqueues the /real/ action to be
--       performed on a job queue.
--
--     * Synchronous exceptions thrown by enqueued @IO@ actions will bring the
--       thread down, which will cause it to asynchronously throw a
--       'TimerWheelDied' exception to the thread that 'create'd it. If you want
--       to catch exceptions and log them, for example, you will have to bake
--       this into the registered actions yourself.
--
-- Below is a depiction of a timer wheel with @6@ timers inserted across @8@
-- spokes, and a resolution of @0.1s@.
--
-- @
--    0s   .1s   .2s   .3s   .4s   .5s   .6s   .7s   .8s
--    +-----+-----+-----+-----+-----+-----+-----+-----+
--    |     | A   |     | B,C | D   |     |     | E,F |
--    +-----+-----+-----+-----+-----+-----+-----+-----+
-- @
data TimerWheel =  TimerWheel
  { wheelSupply :: !Supply
    -- ^ A supply of unique ints.
  , wheelWheel :: !Wheel
    -- ^ The array of collections of timers.
  , wheelThread :: !ThreadId
  }

data Config
  = Config
  { spokes :: !Natural -- ^ Spoke count.
  , resolution :: !(Fixed E6) -- ^ Resolution, in seconds.
  } deriving (Generic, Show)

-- | The timeout thread died.
newtype TimerWheelDied
  = TimerWheelDied SomeException
  deriving (Show)

instance Exception TimerWheelDied where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | The timer wheel config was invalid.
--
-- * @spokes@ must be positive, and less than @maxBound@ of @Int@.
-- * @resolution@ must be positive.
data InvalidTimerWheelConfig
  = InvalidTimerWheelConfig !Config
  deriving (Show)

instance Exception InvalidTimerWheelConfig

-- | Create a timer wheel.
--
-- /Throws./ If the config is invalid, throws 'InvalidTimerWheelConfig'.
--
-- /Throws./ If the timeout thread dies, asynchronously throws 'TimerWheelDied'
-- to the thread that called 'create'.
create :: Config -> IO TimerWheel
create config@(Config { spokes, resolution }) = do
  when (invalidConfig config)
    (throwIO (InvalidTimerWheelConfig config))

  wheel :: Wheel <-
    Wheel.create
      (fromIntegral spokes)
      (fromIntegral (coerce resolution :: Integer))

  supply :: Supply <-
    Supply.new

  thread <- myThreadId

  reaperThread <-
    forkIOWithUnmask $ \unmask ->
      unmask (Wheel.reap wheel)
        `catch` \e ->
          case fromException e of
            Just ThreadKilled -> pure ()
            _                 -> throwTo thread (TimerWheelDied e)

  pure TimerWheel
    { wheelSupply = supply
    , wheelWheel = wheel
    , wheelThread = reaperThread
    }

-- | Tear down a timer wheel by killing the timeout thread.
destroy :: TimerWheel -> IO ()
destroy wheel =
  killThread (wheelThread wheel)

invalidConfig :: Config -> Bool
invalidConfig Config { spokes, resolution } =
  or
    [ spokes == 0
    , spokes > fromIntegral (maxBound :: Int)
    , resolution <= 0
    ]

-- | @register wheel action delay@ registers an action __@action@__ in timer
-- wheel __@wheel@__ to fire after __@delay@__ seconds.
--
-- Returns an action that, when called, attempts to cancel the timer, and
-- returns whether or not it was successful (@False@ means the timer has already
-- fired).
--
-- Subsequent calls to the cancel action have no effect, and continue to return
-- whatever the first result was.
register ::
     TimerWheel -- ^
  -> IO () -- ^ Action
  -> Fixed E6 -- ^ Delay, in seconds
  -> IO (IO Bool)
register wheel action (MkFixed (fromIntegral -> delay)) =
  _register wheel action delay

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ ::
     TimerWheel -- ^
  -> IO () -- ^ Action
  -> Fixed E6 -- ^ Delay, in seconds
  -> IO ()
register_ wheel action delay =
  void (register wheel action delay)

_register :: TimerWheel -> IO () -> Word64 -> IO (IO Bool)
_register wheel action delay = do
  key <- Supply.next (wheelSupply wheel)
  Wheel.insert (wheelWheel wheel) key action delay

_register_ :: TimerWheel -> IO () -> Word64 -> IO ()
_register_ wheel action delay =
  void (_register wheel action delay)

-- | @recurring wheel action delay@ registers an action __@action@__ in timer
-- wheel __@wheel@__ to fire every __@delay@__ seconds.
--
-- Returns an action that, when called, cancels the recurring timer.
recurring ::
     TimerWheel
  -> IO () -- ^ Action
  -> Fixed E6 -- ^ Delay, in seconds
  -> IO (IO ())
recurring wheel action (MkFixed (fromIntegral -> delay)) = mdo
  let
    doAction :: IO ()
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
      writeIORef cancelRef =<<
        _register
          wheel
          doAction
          (delay - Wheel.resolution (wheelWheel wheel))
      action

  cancel :: IO Bool <-
    _register wheel doAction delay

  cancelRef :: IORef (IO Bool) <-
    newIORef cancel

  pure (untilTrue (join (readIORef cancelRef)))

-- | Like 'recurring', but for when you don't intend to cancel the timer.
recurring_ ::
     TimerWheel
  -> IO () -- ^ Action
  -> Fixed E6 -- ^ Delay, in seconds
  -> IO ()
recurring_ wheel action (MkFixed (fromIntegral -> delay)) =
  _register_ wheel doAction delay

  where
    doAction :: IO ()
    doAction = do
      _register_ wheel doAction (delay - Wheel.resolution (wheelWheel wheel))
      action

-- Repeat an IO action until it returns 'True'.
untilTrue :: IO Bool -> IO ()
untilTrue action =
  action >>= \case
    True -> pure ()
    False -> untilTrue action
