{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Fixed         (E6, Fixed(..))
import Data.IORef
import Data.TimerWheel
import System.Random

main :: IO ()
main = do
  do
    putStrLn "Timer wheel runs scheduled actions"
    let n = 10000
    ref <- newIORef (0::Int)
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    putStrLn ("> Inserting " ++ show n ++ " timers")
    replicateM_ n $ do
      delay <- randomRIO (0, 4*1000*1000)
      register_ wheel (modifyIORef' ref (+1)) (MkFixed delay)
    putStrLn "> Sleeping for 5s"
    sleep 5
    readIORef ref `is` n
    destroy wheel

  do
    putStrLn "Timers can be canceled"
    let n = 10000
    ref <- newIORef (0::Int)
    wheel <- create Config { spokes = 4, resolution = 0.25 }
    putStrLn ("> Inserting " ++ show n ++ " timers")
    cancels <-
      replicateM n $ do
        delay <- randomRIO (0, 4*1000*1000)
        register wheel (modifyIORef' ref (+1)) (MkFixed delay)
    putStrLn "> Sleeping for 2s"
    sleep 2
    putStrLn "> Canceling all timers"
    successes <- sequence cancels
    readIORef ref `is` (n - length (filter id successes))
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
    cancel <- register wheel (pure ()) 0.5
    sleep 1
    cancel `is` False
    cancel `is` False
    destroy wheel

  do
    putStrLn "Recurring timers work"
    ref <- newIORef (0::Int)
    wheel <- create Config { spokes = 4, resolution = 0.05 }
    recurring_ wheel (modifyIORef' ref (+1)) 0.15
    sleep 1
    readIORef ref `is` (6::Int)
    destroy wheel

  do
    putStrLn "Recurring timers can be canceled"
    ref <- newIORef (0::Int)
    wheel <- create Config { spokes = 4, resolution = 0.05 }
    cancel <- recurring wheel (modifyIORef' ref (+1)) 0.15
    sleep 1
    cancel
    sleep 1
    readIORef ref `is` (6::Int)
    destroy wheel

class Assert a b where
  is :: a -> b -> IO ()

instance (Eq a, Show a) => Assert (IO a) a where
  is mx y = do
    x <- mx
    unless (x == y) (throwIO (userError (show x ++ " /= " ++ show y)))

sleep :: Fixed E6 -> IO ()
sleep (MkFixed micro) = threadDelay (fromIntegral micro)
