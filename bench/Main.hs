{-# language LambdaCase          #-}
{-# language ScopedTypeVariables #-}

import Control.Concurrent.Async (replicateConcurrently_)
import Control.Monad
import Data.Fixed
import Data.Foldable
import Data.IORef
import GHC.Event (getSystemTimerManager, registerTimeout, unregisterTimeout)
import System.Environment
import System.IO.Unsafe
import System.Random

import qualified Data.TimerWheel.List as TimerWheel.List
import qualified Data.TimerWheel.PSQ as TimerWheel.PSQ
import qualified Data.TimerWheel.SortedList as TimerWheel.SortedList

main :: IO ()
main =
  getArgs >>= \case
    ["wheel-list", n, m] ->
      timerWheelMain
        (read n)
        (read m)
        TimerWheel.List.new
        TimerWheel.List.register
        TimerWheel.List.cancel
    ["wheel-sorted-list", n, m] ->
      timerWheelMain
        (read n)
        (read m)
        TimerWheel.SortedList.new
        TimerWheel.SortedList.register
        TimerWheel.SortedList.cancel
    ["wheel-psq", n, m] ->
      timerWheelMain
        (read n)
        (read m)
        TimerWheel.PSQ.new
        TimerWheel.PSQ.register
        TimerWheel.PSQ.cancel
    ["ghc", n, m] ->
      ghcMain (read n) (read m)
    _ ->
      putStrLn "Expecting args: (wheel-list|wheel-psq|ghc) N M"

timerWheelMain
  :: Int
  -> Int
  -> (Int -> Fixed E6 -> IO wheel)
  -> (Fixed E9 -> IO () -> wheel -> IO timer)
  -> (timer -> IO Bool)
  -> IO ()
timerWheelMain n m new register cancel = do
  wheel <-
    new (2^(16::Int)) (1/100)

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef doublesRef
        writeIORef doublesRef ss
        register (realToFrac s) (pure ()) wheel
    for_ (everyOther timers) cancel

ghcMain :: Int -> Int -> IO ()
ghcMain n m = do
  mgr <- getSystemTimerManager

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef intsRef
        writeIORef intsRef ss
        registerTimeout mgr s (pure ())
    for_ (everyOther timers) (unregisterTimeout mgr)

everyOther :: [a] -> [a]
everyOther = \case
  [] ->
    []
  x:xs ->
    x : otherEvery xs

otherEvery :: [a] -> [a]
otherEvery = \case
  [] ->
    []
  _:xs ->
    everyOther xs

doublesRef :: IORef [Double]
doublesRef =
  unsafePerformIO (newIORef (randomRs (1, 10) (mkStdGen 1)))
{-# NOINLINE doublesRef #-}

intsRef :: IORef [Int]
intsRef =
  unsafePerformIO (newIORef (randomRs (1, 10000000) (mkStdGen 1)))
{-# NOINLINE intsRef #-}
