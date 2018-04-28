{-# language LambdaCase          #-}
{-# language NamedFieldPuns      #-}
{-# language ScopedTypeVariables #-}
{-# language ViewPatterns        #-}

{-# options_ghc -funbox-strict-fields #-}

module Data.TimerWheel
  ( TimerWheel
  , Timer(..)
  , new
  , register
  ) where

import Entry (Entry(..), EntryId)
import Supply (Supply)
import Timestamp (Duration, Timestamp(Timestamp))

import qualified Entry
import qualified Supply
import qualified Timestamp

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Fixed (E9, Fixed, div')
import Data.Foldable
import Data.List (partition)
import Data.Vector (Vector)

import qualified Data.Vector as Vector

-- | A 'TimerWheel' is a vector-of-linked-lists-of timers to fire. It is
-- configured with a /bucket count/ and /accuracy/.
--
-- An immortal reaper thread is used to step through the timer wheel and fire
-- expired timers.
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
--     how accurately timers fire per their registered expiry time.
--
--     For example, with an /accuracy/ of __@1s@__, a timer that expires at
--     __@t = 2.05s@__ will not fire until the reaper thread wakes at
--     __@t = 3s@__.
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
--
data TimerWheel = TimerWheel
  { wheelEpoch :: !Timestamp
  , wheelAccuracy :: !Duration
  , wheelSupply :: !(Supply EntryId)
  , wheelEntries :: !(Vector (TVar [Entry]))
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
new :: Int -> Fixed E9 -> IO TimerWheel
new slots (Timestamp -> accuracy) = do
  epoch :: Timestamp <-
    Timestamp.now

  wheel :: Vector (TVar [Entry]) <-
    Vector.replicateM slots (newTVarIO [])

  void (forkIO (reaper accuracy epoch wheel))

  supply :: Supply EntryId <-
    Supply.new

  pure TimerWheel
    { wheelEpoch = epoch
    , wheelAccuracy = accuracy
    , wheelSupply = supply
    , wheelEntries = wheel
    }

reaper :: Duration -> Timestamp -> Vector (TVar [Entry]) -> IO ()
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

    for_ is $ \k -> do
      let entriesVar :: TVar [Entry]
          entriesVar =
            Vector.unsafeIndex wheel k

      join . atomically $ do
        readTVar entriesVar >>= \case
          [] ->
            pure (pure ())

          entries -> do
            let expired, alive :: [Entry]
                (expired, alive) =
                  partition Entry.isExpired entries

            writeTVar entriesVar $! map' Entry.decrement alive

            -- instance Monoid (IO ()) ;)
            pure (foldMap (ignoreSyncException . entryAction) expired)
    loop j

entriesIn :: Duration -> TimerWheel -> IO (TVar [Entry])
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

  entriesVar :: TVar [Entry] <-
    entriesIn delay wheel

  atomically (modifyTVar' entriesVar (newEntry :))

  entriesVarVar :: TVar (TVar [Entry]) <-
    newTVarIO entriesVar

  let reset :: IO Bool
      reset = do
        newEntriesVar :: TVar [Entry] <-
          entriesIn delay wheel

        atomically $ do
          oldEntriesVar :: TVar [Entry] <-
            readTVar entriesVarVar

          oldEntries :: [Entry] <-
            readTVar oldEntriesVar

          case Entry.delete newEntryId oldEntries of
            (Nothing, _) ->
              pure False

            (Just entry, oldEntries') -> do
              writeTVar oldEntriesVar oldEntries'
              modifyTVar' newEntriesVar (entry :)
              writeTVar entriesVarVar newEntriesVar
              pure True

  let cancel :: IO Bool
      cancel =
        atomically $ do
          oldEntriesVar :: TVar [Entry] <-
            readTVar entriesVarVar

          oldEntries :: [Entry] <-
            readTVar oldEntriesVar

          case Entry.delete newEntryId oldEntries of
            (Nothing, _) ->
              pure False
            (_, oldEntries') -> do
              writeTVar oldEntriesVar oldEntries'
              pure True

  pure Timer
    { reset = reset
    , cancel = cancel
    }

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

map' :: (a -> b) -> [a] -> [b]
map' f = \case
  [] ->
    []

  x:xs ->
    let
      !y = f x
    in
      y : map' f xs

--------------------------------------------------------------------------------
-- Debug functionality

{-
iolock :: MVar ()
iolock =
  unsafePerformIO (newMVar ())
{-# NOINLINE iolock #-}

debug :: [Char] -> IO ()
debug msg =
  withMVar iolock (\_ -> hPutStrLn stderr msg)
-}
