{-# language LambdaCase          #-}
{-# language NamedFieldPuns      #-}
{-# language RecursiveDo         #-}
{-# language ScopedTypeVariables #-}
{-# language ViewPatterns        #-}

{-# options_ghc -funbox-strict-fields #-}

module Data.TimerWheel
  ( -- * Timer wheel
    TimerWheel
  , new
  , register
  , register_
  , recurring
  ) where

import Entries (Entries)
import Supply (Supply)

import qualified Entries as Entries
import qualified Supply

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Fixed (E6, Fixed(MkFixed))
import Data.Foldable
import Data.IORef
import Data.Primitive.MutVar
import Data.Primitive.UnliftedArray
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Prim (RealWorld)

import qualified GHC.Event as GHC

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
-- * The timeout thread has three important properties:
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
  { wheelResolution :: !Word64
    -- ^ The length of time that each entry corresponds to, in nanoseconds.
  , wheelSupply :: !Supply
    -- ^ A supply of unique ints.
  , wheelEntries :: !(UnliftedArray (MutVar RealWorld Entries))
    -- ^ The array of collections of timers.
  , wheelCanary :: !(IORef ())
    -- ^ An IORef to which we attach a finalizer that kills the reaper thread
    -- when the wheel is garbage collected.
  }

-- | @new n s@ creates a 'TimerWheel' with __@n@__ spokes and a resolution of
-- __@s@__ seconds.
new :: Int -> Fixed E6 -> IO TimerWheel
new slots (MkFixed (fromInteger -> resolution)) = do
  wheel :: UnliftedArray (MutVar RealWorld Entries) <- do
    wheel :: MutableUnliftedArray RealWorld (MutVar RealWorld Entries) <-
      unsafeNewUnliftedArray slots
    for_ [0..slots-1] $ \i ->
      writeUnliftedArray wheel i =<< newMutVar Entries.empty
    freezeUnliftedArray wheel 0 slots

  supply :: Supply <-
    Supply.new

  reaperId :: ThreadId <-
    forkIO (reaper resolution wheel)

  canary :: IORef () <-
    newIORef ()

  _ <-
    mkWeakIORef canary (killThread reaperId)

  pure TimerWheel
    { wheelResolution = fromIntegral resolution * 1000
    , wheelSupply = supply
    , wheelEntries = wheel
    , wheelCanary = canary
    }

reaper :: Int -> UnliftedArray (MutVar RealWorld Entries) -> IO ()
reaper resolution wheel = do
  manager :: GHC.TimerManager <-
    GHC.getSystemTimerManager
  loop manager 0
 where
  -- Reaper loop: we haven't yet run the entries at index 'i'.
  loop :: GHC.TimerManager -> Int -> IO ()
  loop manager i = do
    -- Sleep until the roughly the next bucket.
    waitVar :: MVar () <-
      newEmptyMVar
    mask_ $ do
      key :: GHC.TimeoutKey <-
        GHC.registerTimeout manager resolution (putMVar waitVar ())
      takeMVar waitVar `onException` GHC.unregisterTimeout manager key

    now :: Word64 <-
      getMonotonicTimeNSec

    -- Figure out which bucket we're in. Usually this will be 'i+1', but maybe
    -- we were scheduled a bit early and ended up in 'i', or maybe running the
    -- entries in bucket 'i-1' took a long time and we slept all the way to
    -- 'i+2'. In any case, we should run the entries in buckets
    -- up-to-but-not-including this one, beginning with bucket 'i'.

    let
      j :: Int
      j =
        fromIntegral (now `div` (fromIntegral resolution * 1000))
          `mod` sizeofUnliftedArray wheel

    let
      is :: [Int]
      is =
        if j >= i
          then
            [i .. j-1]
          else
            [i .. sizeofUnliftedArray wheel - 1] ++ [0 .. j-1]

    -- To actually run the entries in a bucket, partition them into expired
    -- (count == 0) and alive (count > 0). Run the expired entries and
    -- decrement the alive entries' counts by 1.

    for_ is $ \k -> do
      let
        entriesRef :: MutVar RealWorld Entries
        entriesRef =
          indexUnliftedArray wheel k

      join
        (atomicModifyMutVar' entriesRef
          (\entries ->
            if Entries.null entries
              then
                (entries, pure ())
              else
                case Entries.squam entries of
                  (expired, alive) ->
                    (alive, sequence_ expired)))
    loop manager j

-- | @register n m w@ registers an action __@m@__ in timer wheel __@w@__ to fire
-- after __@n@__ seconds.
--
-- Returns an action that, when called, attempts to cancel the timer, and
-- returns whether or not it was successful (@False@ means the timer has already
-- fired).
register :: Fixed E6 -> IO () -> TimerWheel -> IO (IO Bool)
register (MkFixed ((*1000) . fromIntegral -> delay)) action wheel = do
  newEntryId :: Int <-
    Supply.next (wheelSupply wheel)

  entriesVar :: MutVar RealWorld Entries <-
    entriesIn delay wheel

  atomicModifyMutVar' entriesVar
    (\entries ->
      (Entries.insert newEntryId entryCount action entries, ()))

  pure $ do
    atomicModifyMutVar' entriesVar
      (\entries ->
        case Entries.delete newEntryId entries of
          Nothing ->
            (entries, False)
          Just entries' ->
            (entries', True))
 where
  entryCount :: Word64
  entryCount =
    delay `div`
      (fromIntegral (sizeofUnliftedArray (wheelEntries wheel))
        * wheelResolution wheel)

-- | Like 'register', but for when you don't intend to cancel the timer.
register_ :: Fixed E6 -> IO () -> TimerWheel -> IO ()
register_ delay action wheel =
  void (register delay action wheel)

-- | @recurring n m w@ registers an action __@m@__ in timer wheel __@w@__ to
-- fire every __@n@__ seconds.
--
-- Returns an action that, when called, cancels the recurring timer.
recurring :: Fixed E6 -> IO () -> TimerWheel -> IO (IO ())
recurring delay action wheel = mdo
  cancel :: IO Bool <-
    register delay (action' cancelRef) wheel
  cancelRef :: IORef (IO Bool) <-
    newIORef cancel
  pure (untilTrue (join (readIORef cancelRef)))
 where
  action' :: IORef (IO Bool) -> IO ()
  action' cancelRef = do
    action
    cancel :: IO Bool <-
      register delay (action' cancelRef) wheel
    writeIORef cancelRef cancel

-- | @entriesIn delay wheel@ returns the bucket in @wheel@ that corresponds to
-- @delay@ nanoseconds from now.
entriesIn :: Word64 -> TimerWheel -> IO (MutVar RealWorld Entries)
entriesIn delay TimerWheel{wheelResolution, wheelEntries} = do
  now :: Word64 <-
    getMonotonicTimeNSec
  pure (index ((now+delay) `div` wheelResolution))
 where
  index :: Word64 -> MutVar RealWorld Entries
  index i =
    indexUnliftedArray wheelEntries
      (fromIntegral i `rem` sizeofUnliftedArray wheelEntries)

-- | Repeat an IO action until it returns 'True'.
untilTrue :: IO Bool -> IO ()
untilTrue action =
  action >>= \case
    True ->
      pure ()
    False ->
      untilTrue action
