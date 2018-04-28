{-# language CPP                 #-}
{-# language LambdaCase          #-}
{-# language NamedFieldPuns      #-}
{-# language ScopedTypeVariables #-}
{-# language ViewPatterns        #-}

{-# options_ghc -funbox-strict-fields #-}

module Data.TimerWheel
  ( TimerWheel
  , Timer(..)
  , new
  , stop
  , register
  , register_
  ) where

import EntriesPSQ (Entries)
-- import Entries (Entries)
import Entry (Entry(..), EntryId)
import Supply (Supply)
import Timestamp (Duration, Timestamp(Timestamp))

import qualified EntriesPSQ as Entries
-- import qualified Entries as Entries
import qualified Entry
import qualified Supply
import qualified Timestamp

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Fixed (E6, E9, Fixed, div')
import Data.Foldable
import Data.Vector (Vector)
import System.IO.Unsafe

import qualified Data.Vector as Vector

-- | A 'TimerWheel' is a vector-of-linked-lists-of timers to fire. It is
-- configured with a /bucket count/ and /accuracy/.
--
-- An reaper thread is used to step through the timer wheel and fire expired
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
  { wheelEpoch :: !Timestamp
  , wheelAccuracy :: !Duration
  , wheelSupply :: !(Supply EntryId)
  , wheelEntries :: !(Vector (TVar Entries))
  , wheelThread :: !ThreadId
  }

data Timer = Timer
  { reset :: IO Bool
    -- ^ Reset a 'Timer'. This is equivalent to atomically 'cancel'ing it, then
    -- 'register'ing a new one with the same delay and action. Returns 'False'
    -- if the 'Timer' has already fired.
  , cancel :: IO Bool
    -- ^ Cancel a 'Timer'. Returns 'False' if the 'Timer' has already fired.
  }

-- | @new n s@ creates a 'TimerWheel' with __@n@__ buckets and an accuracy of
-- __@s@__ seconds.
new :: Int -> Fixed E6 -> IO TimerWheel
new slots (realToFrac -> accuracy) = do
  epoch :: Timestamp <-
    Timestamp.now

  wheel :: Vector (TVar Entries) <-
    Vector.replicateM slots (newTVarIO Entries.empty)

  reaperId :: ThreadId <-
    forkIO (reaper accuracy epoch wheel)

  supply :: Supply EntryId <-
    Supply.new

  pure TimerWheel
    { wheelEpoch = epoch
    , wheelAccuracy = accuracy
    , wheelSupply = supply
    , wheelEntries = wheel
    , wheelThread = reaperId
    }

stop :: TimerWheel -> IO ()
stop wheel =
  killThread (wheelThread wheel)

reaper :: Duration -> Timestamp -> Vector (TVar Entries) -> IO ()
reaper accuracy epoch wheel =
  loop 0
 where
  -- Reaper loop: we haven't yet run the entries at index 'i'.
  loop :: Int -> IO ()
  loop i = do
    -- Sleep until the roughly the next bucket.
    threadDelay (Timestamp.micro accuracy)

    elapsed :: Timestamp <-
      Timestamp.since epoch

    -- Figure out which bucket we're in. Usually this will be 'i+1', but maybe
    -- we were scheduled a bit early and ended up in 'i', or maybe running the
    -- entries in bucket 'i-1' took a long time and we slept all the way to
    -- 'i+2'. In any case, we should run the entries in buckets
    -- up-to-but-not-including this one, beginning with bucket 'i'.

    let j :: Int
        j =
          fromInteger (elapsed `div'` accuracy) `mod` Vector.length wheel

    let is :: [Int]
        is =
          if j >= i
            then
              [i .. j-1]
            else
              [i .. Vector.length wheel - 1] ++ [0 .. j-1]

    -- To actually run the entries in a bucket, partition them into expired
    -- (count == 0) and alive (count > 0). Run the expired entries and
    -- decrement the alive entries' counts by 1.

    debug (putStrLn ("Firing " ++ show (length is) ++ " buckets"))

    for_ is $ \k -> do
      let entriesVar :: TVar Entries
          entriesVar =
            Vector.unsafeIndex wheel k

      join . atomically $ do
        entries :: Entries <-
          readTVar entriesVar

        if Entries.null entries
          then
            pure (pure ())
          else do
            let (expired, alive) = Entries.squam entries
            writeTVar entriesVar alive

            pure $ do
              debug $
                putStrLn $
                  "  " ++ show (length expired) ++ " expired, "
                    ++ show (Entries.size alive) ++ " alive"
              -- instance Monoid (IO ()) ;)
              foldMap ignoreSyncException expired
    loop j

entriesIn :: Duration -> TimerWheel -> IO (TVar Entries)
entriesIn delay TimerWheel{wheelAccuracy, wheelEpoch, wheelEntries} = do
  elapsed :: Duration <-
    Timestamp.since wheelEpoch
  pure (index ((elapsed+delay) `div'` wheelAccuracy) wheelEntries)

-- | @register n m w@ an action @m@ in wheel @w@ to fire after @n@ seconds.
register :: Fixed E9 -> IO () -> TimerWheel -> IO Timer
register (Timestamp -> delay) action wheel = do
  newEntryId :: EntryId <-
    Supply.next (wheelSupply wheel)

  let newEntry :: Entry
      newEntry =
        Entry
          { entryId = newEntryId
          , entryCount = wheelEntryCount delay wheel
          , entryAction = action
          }

  entriesVar :: TVar Entries <-
    entriesIn delay wheel

  atomically (modifyTVar' entriesVar (Entries.insert newEntry))

  entriesVarVar :: TVar (TVar Entries) <-
    newTVarIO entriesVar

  let reset :: IO Bool
      reset = do
        newEntriesVar :: TVar Entries <-
          entriesIn delay wheel

        atomically $ do
          oldEntriesVar :: TVar Entries <-
            readTVar entriesVarVar

          oldEntries :: Entries <-
            readTVar oldEntriesVar

          case Entries.delete newEntryId oldEntries of
            (Nothing, _) ->
              pure False

            (Just entry, oldEntries') -> do
              writeTVar oldEntriesVar oldEntries'
              modifyTVar' newEntriesVar (Entries.insert entry)
              writeTVar entriesVarVar newEntriesVar
              pure True

  let cancel :: IO Bool
      cancel =
        atomically $ do
          oldEntriesVar :: TVar Entries <-
            readTVar entriesVarVar

          oldEntries :: Entries <-
            readTVar oldEntriesVar

          case Entries.delete newEntryId oldEntries of
            (Nothing, _) ->
              pure False
            (_, oldEntries') -> do
              writeTVar oldEntriesVar oldEntries'
              pure True

  pure Timer
    { reset = reset
    , cancel = cancel
    }

-- | Like 'register', but for when you don't care to 'cancel' or 'reset' the
-- timer.
register_ :: Fixed E6 -> IO () -> TimerWheel -> IO ()
register_ (realToFrac -> delay) action wheel = do
  newEntryId :: EntryId <-
    Supply.next (wheelSupply wheel)

  let newEntry :: Entry
      newEntry =
        Entry
          { entryId = newEntryId
          , entryCount = wheelEntryCount delay wheel
          , entryAction = action
          }

  entriesVar :: TVar Entries <-
    entriesIn delay wheel

  atomically (modifyTVar' entriesVar (Entries.insert newEntry))

wheelEntryCount :: Duration -> TimerWheel -> Int
wheelEntryCount delay TimerWheel{wheelAccuracy, wheelEntries} =
  fromInteger
    (delay `div'`
      (fromIntegral (Vector.length wheelEntries) * wheelAccuracy))

ignoreSyncException :: IO () -> IO ()
ignoreSyncException action =
  action `catch` \ex ->
    case fromException ex of
      Just (SomeAsyncException _) ->
        throwIO ex
      _ ->
        pure ()

index :: Integer -> Vector a -> a
index i v =
  Vector.unsafeIndex v (fromIntegral i `rem` Vector.length v)

--------------------------------------------------------------------------------
-- Debug functionality

#ifdef DEBUG
iolock :: MVar ()
iolock =
  unsafePerformIO (newMVar ())
{-# NOINLINE iolock #-}
#endif

debug :: IO () -> IO ()
debug action =
#ifdef DEBUG
  withMVar iolock (\_ -> action)
#else
  pure ()
#endif
