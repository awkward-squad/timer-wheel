{-# LANGUAGE BlockArguments, DeriveAnyClass, DerivingStrategies, FlexibleInstances, MultiParamTypeClasses #-}

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
    with Config { spokes = 4, resolution = 0.05 } \wheel -> do
      var <- newEmptyMVar
      let n = 1000
      replicateM_ n (register_ wheel (putMVar var ()) 0)
      replicateM_ n (takeMVar var)

  do
    putStrLn "Timers can be canceled"
    var <- newEmptyMVar
    with Config { spokes = 4, resolution = 0.05 } \wheel -> do
      let n = 1000
      cancels <- replicateM n (register wheel (putMVar var ()) 0)
      successes <- sequence (take (n `div` 2) cancels)
      replicateM_ (n - length (filter id successes)) (takeMVar var)

  do
    putStrLn "Re-calling a successful cancel works"
    with Config { spokes = 4, resolution = 0.05 } \wheel -> do
      cancel <- register wheel (pure ()) 1
      cancel `is` True
      cancel `is` True

  do
    putStrLn "Re-calling a failed cancel works"
    with Config { spokes = 4, resolution = 0.05 } \wheel -> do
      var <- newEmptyMVar
      cancel <- register wheel (putMVar var ()) 0
      takeMVar var
      cancel `is` False
      cancel `is` False

  do
    putStrLn "Recurring timers work"
    with Config { spokes = 4, resolution = 0.05 } \wheel -> do
      canary <- newIORef () -- kept alive only by timer
      weakCanary <- mkWeakIORef canary (pure ())
      var <- newEmptyMVar
      cancel <- recurring wheel (readIORef canary >> putMVar var ()) 0
      replicateM_ 2 (takeMVar var)
      cancel -- should drop reference canary after a GC
      performGC
      (isJust <$> deRefWeak weakCanary) `is` False

  do
    putStrLn "`with` re-throws exception from background thread"
    catch
      (with Config { spokes = 4, resolution = 0.05 } \wheel -> do
        var <- newEmptyMVar
        register_ wheel (throwIO Bye >> putMVar var ()) 0
        takeMVar var
        throwIO (userError "fail"))
      (\ex ->
        case fromException ex of
          Just Bye -> pure ()
          _ -> throwIO ex)

data Bye = Bye
  deriving stock (Show)
  deriving anyclass (Exception)

class Assert a b where
  is :: a -> b -> IO ()

instance (Eq a, Show a) => Assert (IO a) a where
  is mx y = do
    x <- mx
    unless (x == y) (throwIO (userError (show x ++ " /= " ++ show y)))
