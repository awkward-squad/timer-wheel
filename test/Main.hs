{-# LANGUAGE BlockArguments, FlexibleInstances, MultiParamTypeClasses #-}

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Maybe (isJust)
import Data.TimerWheel
import System.Mem (performGC)
import System.Mem.Weak (deRefWeak)

main :: IO ()
main = do
  do
    putStrLn "Timer wheel runs scheduled actions"
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    var <- newEmptyMVar
    let n = 1000
    replicateM_ n (register_ wheel (putMVar var ()) 0)
    replicateM_ n (takeMVar var)
    destroy wheel

  do
    putStrLn "Timers can be canceled"
    var <- newEmptyMVar
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    let n = 1000
    cancels <- replicateM n (register wheel (putMVar var ()) 0)
    successes <- sequence (take (n `div` 2) cancels)
    replicateM_ (n - length (filter id successes)) (takeMVar var)
    destroy wheel

  do
    putStrLn "Re-calling a successful cancel works"
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    cancel <- register wheel (pure ()) 1
    cancel `is` True
    cancel `is` True
    destroy wheel

  do
    putStrLn "Re-calling a failed cancel works"
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    var <- newEmptyMVar
    cancel <- register wheel (putMVar var ()) 0
    takeMVar var
    cancel `is` False
    cancel `is` False
    destroy wheel

  do
    putStrLn "Recurring timers work"
    wheel <- create Config { spokes = 4, resolution = 0.05 }
    canary <- newIORef () -- kept alive only by timer
    weakCanary <- mkWeakIORef canary (pure ())
    var <- newEmptyMVar
    cancel <- recurring wheel (readIORef canary >> putMVar var ()) 0
    replicateM_ 2 (takeMVar var)
    cancel -- should drop reference canary after a GC
    performGC
    (isJust <$> deRefWeak weakCanary) `is` False
    destroy wheel

class Assert a b where
  is :: a -> b -> IO ()

instance (Eq a, Show a) => Assert (IO a) a where
  is mx y = do
    x <- mx
    unless (x == y) (throwIO (userError (show x ++ " /= " ++ show y)))
