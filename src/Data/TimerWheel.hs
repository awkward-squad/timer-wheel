{-# language MagicHash           #-}
{-# language NamedFieldPuns      #-}
{-# language ScopedTypeVariables #-}
{-# language ViewPatterns        #-}

{-# options_ghc -funbox-strict-fields #-}

module Data.TimerWheel
  ( TimerWheel
  , new
  , stop
  , register
  , register_
  ) where

import Debug (debug)
import Entries (Entries)
import Supply (Supply)

import qualified Entries as Entries
import qualified Supply

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Fixed
import Data.Foldable
import Data.Primitive.MutVar
import Data.Primitive.UnliftedArray
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Prim (RealWorld)

import qualified GHC.Event as GHC

-- | A 'TimerWheel' is a vector-of-collections-of timers to fire. It is
-- configured with a /bucket count/ and /accuracy/.
--
-- A reaper thread is used to step through the timer wheel and fire expired
-- timers.
--
-- * The /bucket count/ determines the size of the timer vector.
--
--     * A __larger__ bucket count will result in __less__ 'register' contention
--       at each individual bucket and require __more__ memory to store the
--       timer wheel.
--
--     * A __smaller__ bucket count will result in __more__ 'register'
--       contention at each individual bucket and require __less__ memory to
--       store the timer wheel.
--
-- * The /accuracy/ determines how often the reaper thread wakes, and thus
--   how accurately timers fire per their registered expiry time.
--
--   For example, with an /accuracy/ of __@1s@__, a timer that expires at
--   __@t = 2.05s@__ will not fire until the reaper thread wakes at
--   __@t = 3s@__.
--
--     * A __larger__ accuracy will result in __less__ accurate timers and
--       require __less__ work by the reaper thread.
--
--     * A __smaller__ accuracy will result in __more__ accurate timers and
--       require __more__ work by the reaper thread.
--
-- * The reaper thread has two important properties:
--
--     * There is only one, and it fires expired timers synchronously. If your
--       timer actions are very cheap and execute quicky, 'register' them
--       directly. Otherwise, consider registering an action that enqueues the
--       real action to be performed on a queue.
--
--     * Synchronous exceptions are completely ignored. If you want to handle
--       exceptions, bake that logic into the registered action itself.
--
-- Below is an example of a timer wheel with __@8@__ buckets and an accuracy of
-- __@0.1s@__.
--
-- @
--    0s   .1s   .2s   .3s   .4s   .5s   .6s   .7s   .8s
--    +-----+-----+-----+-----+-----+-----+-----+-----+
--    |     |     |     |     |     |     |     |     |
--    +-|---+-|---+-|---+-|---+-|---+-|---+-|---+-|---+
--      |     |     |     |     |     |     |     |
--      |     |     |     |     |     |     |     |
--      ∅     A     ∅     B,C   D     ∅     ∅     E,F,G
-- @
--
-- A timer registered to fire at __@t = 1s@__ would be bucketed into index
-- __@2@__ and would expire on the second "lap" through the wheel when the
-- reaper thread advances to index __@3@__.
data TimerWheel = TimerWheel
  { wheelAccuracy :: !Word64
  , wheelSupply :: !Supply
  , wheelEntries :: !(UnliftedArray (MutVar RealWorld Entries))
  , wheelThread :: !ThreadId
  }

-- | @new n s@ creates a 'TimerWheel' with __@n@__ buckets and an accuracy of
-- __@s@__ seconds.
new :: Int -> Fixed E6 -> IO TimerWheel
new slots (MkFixed (fromInteger -> accuracy)) = do
  wheel :: UnliftedArray (MutVar RealWorld Entries) <- do
    wheel :: MutableUnliftedArray RealWorld (MutVar RealWorld Entries) <-
      unsafeNewUnliftedArray slots
    for_ [0..slots-1] $ \i ->
      writeUnliftedArray wheel i =<< newMutVar Entries.empty
    freezeUnliftedArray wheel 0 slots

  reaperId :: ThreadId <-
    forkIO (reaper accuracy wheel)

  supply :: Supply <-
    Supply.new

  pure TimerWheel
    { wheelAccuracy = fromIntegral accuracy * 1000
    , wheelSupply = supply
    , wheelEntries = wheel
    , wheelThread = reaperId
    }

stop :: TimerWheel -> IO ()
stop wheel =
  killThread (wheelThread wheel)

reaper :: Int -> UnliftedArray (MutVar RealWorld Entries) -> IO ()
reaper accuracy wheel = do
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
        GHC.registerTimeout
          manager
          accuracy
          (putMVar waitVar ())
      takeMVar waitVar `onException` GHC.unregisterTimeout manager key

    now :: Word64 <-
      getMonotonicTimeNSec

    -- Figure out which bucket we're in. Usually this will be 'i+1', but maybe
    -- we were scheduled a bit early and ended up in 'i', or maybe running the
    -- entries in bucket 'i-1' took a long time and we slept all the way to
    -- 'i+2'. In any case, we should run the entries in buckets
    -- up-to-but-not-including this one, beginning with bucket 'i'.

    let j :: Int
        j =
          fromIntegral (now `div` (fromIntegral accuracy * 1000))
            `mod` sizeofUnliftedArray wheel

    let is :: [Int]
        is =
          if j >= i
            then
              [i .. j-1]
            else
              [i .. sizeofUnliftedArray wheel - 1] ++ [0 .. j-1]

    -- To actually run the entries in a bucket, partition them into expired
    -- (count == 0) and alive (count > 0). Run the expired entries and
    -- decrement the alive entries' counts by 1.

    debug (putStrLn ("Firing " ++ show (length is) ++ " buckets"))

    for_ is $ \k -> do
      let entriesRef :: MutVar RealWorld Entries
          entriesRef =
            indexUnliftedArray wheel k

      join
        (atomicModifyMutVar' entriesRef
          (\entries ->
            if Entries.null entries
              then
                (entries, pure () :: IO ())
              else
                case Entries.squam entries of
                  (expired, alive) ->
                    (alive, do
                      debug $
                        putStrLn $
                          "  " ++ show (length expired) ++ " expired, "
                            ++ show (Entries.size alive) ++ " alive"
                      -- instance Monoid (IO ()) ;)
                      foldMap ignoreSyncException expired)))
    loop manager j

entriesIn :: Word64 -> TimerWheel -> IO (MutVar RealWorld Entries)
entriesIn delay TimerWheel{wheelAccuracy, wheelEntries} = do
  now :: Word64 <-
    getMonotonicTimeNSec
  pure (index ((now+delay) `div` wheelAccuracy) wheelEntries)

-- | @register n m w@ an action @m@ in wheel @w@ to fire after @n@ microseconds.
-- Returns an action that, when called, attempts to cancel the timer, and
-- returns whether or not it was successful.
register :: Int -> IO () -> TimerWheel -> IO (IO Bool)
register ((*1000) . fromIntegral -> delay) action wheel = do
  newEntryId :: Int <-
    Supply.next (wheelSupply wheel)

  entriesVar :: MutVar RealWorld Entries <-
    entriesIn delay wheel

  atomicModifyMutVar' entriesVar
    (\entries ->
      (Entries.insert newEntryId (wheelEntryCount delay wheel) action entries, ()))

  pure $ do
    atomicModifyMutVar' entriesVar
      (\entries ->
        case Entries.delete newEntryId entries of
          Nothing ->
            (entries, False)
          Just entries' ->
            (entries', True))

-- | Like 'register', but for when you don't care to cancel the timer.
register_ :: Int -> IO () -> TimerWheel -> IO ()
register_ delay action wheel =
  void (register delay action wheel)

wheelEntryCount :: Word64 -> TimerWheel -> Word64
wheelEntryCount delay TimerWheel{wheelAccuracy, wheelEntries} =
  delay `div` (fromIntegral (sizeofUnliftedArray wheelEntries) * wheelAccuracy)

ignoreSyncException :: IO () -> IO ()
ignoreSyncException action =
  action `catch` \ex ->
    case fromException ex of
      Just (SomeAsyncException _) ->
        throwIO ex
      _ ->
        pure ()

index :: PrimUnlifted a => Word64 -> UnliftedArray a -> a
index i v =
  indexUnliftedArray v (fromIntegral i `rem` sizeofUnliftedArray v)
