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
import Data.Fixed (Nano, div')
import Data.Foldable
import Data.List (partition)
import Data.Vector (Vector)

import qualified Data.Vector as Vector

data TimerWheel = TimerWheel
  { wheelEpoch :: !Timestamp
  , wheelAccuracy :: !Duration
  , wheelSupply :: !(Supply EntryId)
  , wheelEntries :: !(Vector (TVar [Entry]))
  }

data Timer = Timer
  { reset :: IO Bool
  , cancel :: IO Bool
  }

new :: Int -> Nano -> IO TimerWheel
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

register :: Nano -> IO () -> TimerWheel -> IO Timer
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
