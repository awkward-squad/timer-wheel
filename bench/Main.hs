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

import qualified Data.TimerWheel as TimerWheel

main :: IO ()
main =
  getArgs >>= \case
    ["wheel", n, m] -> timerWheelMain (read n) (read m)
    ["ghc", n, m] -> ghcMain (read n) (read m)

timerWheelMain :: Int -> Int -> IO ()
timerWheelMain n m = do
  wheel <- TimerWheel.new (2^(16::Int)) (1/10)

  replicateConcurrently_ n $ do
    timers <-
      replicateM m $ do
        s:ss <- readIORef doublesRef
        writeIORef doublesRef ss
        TimerWheel.register (realToFrac s) (pure ()) wheel
    for_ timers TimerWheel.cancel

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
