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
  , InvalidTimerWheelConfig(..)
  , TimerWheelDied(..)
  ) where

import Entries (Entries)
import Supply  (Supply)
import Utils
import Wheel   (Wheel)

import qualified Entries
import qualified Supply
import qualified Wheel

import Control.Concurrent      (ThreadId, forkIOWithUnmask, killThread,
                                myThreadId, throwTo)
import Control.Concurrent.MVar
import Control.Exception       (Exception(fromException, toException),
                                SomeException, asyncExceptionFromException,
                                asyncExceptionToException, catch, throwIO)
import Control.Monad           (join, void, when)
import Data.Coerce             (coerce)
import Data.Fixed              (E6, Fixed(MkFixed))
import Data.IORef              (IORef, newIORef, readIORef, writeIORef)
import Data.Primitive.MutVar   (MutVar, atomicModifyMutVar')
import Data.Word               (Word64)
-- import GHC.Base                     (IO(IO), mkWeak#)
import GHC.Generics (Generic)
import GHC.Prim     (RealWorld)
-- import GHC.Weak         (Weak(Weak), deRefWeak)
import Numeric.Natural  (Natural)
import System.IO.Unsafe (unsafeInterleaveIO)

-- | A 'TimerWheel' is a vector-of-collections-of timers to fire. It is
-- configured with a /spoke count/ and /resolution/. A timeout thread is spawned
-- to step through the timer wheel and fire expired timers at regular intervals.
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
--       thread down, and no more timeouts will ever fire. If you want to catch
--       exceptions and log them, for example, you will have to bake this into
--       the registered actions yourself.
--
--     * The timeout thread will asynchronously propagate any exception to the
--       thread that created the timer wheel, wrapped inside a 'TimerWheelDied'
--       constructor. This is an attempt at immediately notifying your
--       application when something has gone horribly wrong, so make sure this
--       exception doesn't get lost (as it would if it merely brings down a
--       forked thread).
--
--     * The life of the timeout thread is scoped to the life of the timer
--       wheel. When the timer wheel is garbage collected, the timeout thread
--       will automatically stop doing work, and die gracefully.
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
  { -- ^ The length of time that each entry corresponds to, in nanoseconds.
    wheelSupply :: !Supply
    -- ^ A supply of unique ints.
  , wheelWheel :: !Wheel
    -- ^ The array of collections of timers.
  , wheelThread :: !ThreadId
  }

data Config
  = Config
  { spokes :: !Natural -- ^ Spoke count.
  , resolution :: !(Fixed E6) -- ^ Resolution, in seconds.
  } deriving stock (Generic, Show)

newtype TimerWheelDied
  = TimerWheelDied SomeException
  deriving (Show)

instance Exception TimerWheelDied where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

data InvalidTimerWheelConfig
  = InvalidTimerWheelConfig !Config
  deriving (Show)

instance Exception InvalidTimerWheelConfig

-- | Create a timer wheel.
--
-- /Throws./ If @spokes == 0@, @spokes > maxBound \@Int@ or @resolution <= 0@,
-- throws an 'InvalidTimerWheelConfig' exception.
--
-- /Throws./ If the timeout thread dies, asynchronously throws 'TimerWheelDied'
-- to the thread that called 'new'.
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
        `catch` \e -> throwTo thread (TimerWheelDied e)

  pure TimerWheel
    { wheelSupply = supply
    , wheelWheel = wheel
    , wheelThread = reaperThread
    }

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
register :: TimerWheel -> IO () -> Fixed E6 -> IO (IO Bool)
register wheel action (MkFixed (fromIntegral -> delay)) = do
  newEntryId :: Int <-
    Supply.next (wheelSupply wheel)

  now :: Word64 <-
    getMonotonicMicros

  let
    bucketVar :: MutVar RealWorld Entries
    bucketVar =
      Wheel.bucket (wheelWheel wheel) now

  atomicModifyMutVar' bucketVar
    (\entries ->
      (Entries.insert newEntryId count (const action entries) entries, ()))

  canceledVar :: MVar (Maybe Bool) <-
    unsafeInterleaveIO (newMVar Nothing)

  pure $ do
    modifyMVar canceledVar $ \result -> do
      canceled <-
        maybe
          (atomicModifyMutVar' bucketVar
            (\entries ->
              maybe
                (entries, False)
                (, True)
                (Entries.delete newEntryId entries)))
          pure
          result
      pure (Just canceled, canceled)

  where
    count :: Word64
    count =
      delay `div` Wheel.lenMicros (wheelWheel wheel)

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ :: TimerWheel -> IO () -> Fixed E6 -> IO ()
register_ wheel action delay =
  void (register wheel action delay)

-- | @recurring wheel delay action@ registers an action __@action@__ in timer
-- wheel __@wheel@__ to fire every __@delay@__ seconds.
--
-- Returns an action that, when called, cancels the recurring timer.
recurring :: TimerWheel -> IO () -> Fixed E6 -> IO (IO ())
recurring wheel action delay = mdo
  cancel <- register wheel (action' cancelRef) delay
  cancelRef <- newIORef cancel
  pure (untilTrue (join (readIORef cancelRef)))

  where
    action' :: IORef (IO Bool) -> IO ()
    action' cancelRef = do
      action
      cancel <- register wheel (action' cancelRef) delay
      writeIORef cancelRef cancel

-- Repeat an IO action until it returns 'True'.
untilTrue :: IO Bool -> IO ()
untilTrue action =
  action >>= \case
    True -> pure ()
    False -> untilTrue action
