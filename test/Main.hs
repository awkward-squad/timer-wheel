{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Fixed         (E6, Fixed(..))
import Data.IORef
import Data.TimerWheel
import System.Mem

main :: IO ()
main = do
  do
    putStrLn "timer wheel runs scheduled actions"
    ref <- newIORef False
    wheel <- create Config { spokes = 8, resolution = 0.25 }
    register_ wheel (writeIORef ref True) 0.5
    performGC
    sleep 0.85
    readIORef ref `is` True
    destroy wheel

  -- do
  --   putStrLn "timeout thread stops working when wheel is garbage collected"
  --   ref <- newIORef False
  --   wheel <- new 8 0.25
  --   register_ 0.5 (writeIORef ref True) wheel
  --   performGC
  --   sleep 0.85
  --   readIORef ref `is` False

class Assert a b where
  is :: a -> b -> IO ()

instance Eq a => Assert (IO a) a where
  is mx y = do
    x <- mx
    unless (x == y) (throwIO (userError "Assert failed"))

sleep :: Fixed E6 -> IO ()
sleep (MkFixed micro) = threadDelay (fromIntegral micro)
