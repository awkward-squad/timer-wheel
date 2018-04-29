{-# language LambdaCase          #-}
{-# language ScopedTypeVariables #-}

import Control.Concurrent.Async
import Control.Monad
import Data.Foldable
import Data.IORef
import GHC.Event
import System.Environment
import System.IO.Unsafe
import System.Random

import qualified Data.TimerWheel.List as TimerWheel.List
import qualified Data.TimerWheel.PSQ as TimerWheel.PSQ

main :: IO ()
main =
  getArgs >>= \case
    ["wheel-list", n, m] ->
      timerWheelListMain (read n) (read m)
    ["wheel-psq", n, m] ->
      timerWheelPsqMain (read n) (read m)
    ["ghc", n, m] ->
      ghcMain (read n) (read m)
    _ ->
      putStrLn "Expecting args: (wheel-list|wheel-psq|ghc) N M"

timerWheelListMain :: Int -> Int -> IO ()
timerWheelListMain n m = do
  wheel <- TimerWheel.List.new (2^(16::Int)) (1/10)

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef doublesRef
        writeIORef doublesRef ss
        TimerWheel.List.register (realToFrac s) (pure ()) wheel
    for_ timers TimerWheel.List.cancel

timerWheelPsqMain :: Int -> Int -> IO ()
timerWheelPsqMain n m = do
  wheel <- TimerWheel.PSQ.new (2^(16::Int)) (1/10)

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef doublesRef
        writeIORef doublesRef ss
        TimerWheel.PSQ.register (realToFrac s) (pure ()) wheel
    for_ timers TimerWheel.PSQ.cancel

ghcMain :: Int -> Int -> IO ()
ghcMain n m = do
  mgr <- getSystemTimerManager

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef intsRef
        writeIORef intsRef ss
        registerTimeout mgr s (pure ())
    for_ timers (unregisterTimeout mgr)

doublesRef :: IORef [Double]
doublesRef =
  unsafePerformIO (newIORef (randomRs (1, 10) (mkStdGen 1)))
{-# NOINLINE doublesRef #-}

intsRef :: IORef [Int]
intsRef =
  unsafePerformIO (newIORef (randomRs (1, 10000000) (mkStdGen 1)))
{-# NOINLINE intsRef #-}
