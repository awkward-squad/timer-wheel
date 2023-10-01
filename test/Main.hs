import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import GHC.Conc (atomically)
import qualified Ki
import qualified System.Random as Random
import TimerWheel
import Prelude

main :: IO ()
main = do
  Ki.scoped \scope -> do
    mainThread <- Ki.fork scope main1
    atomically (Ki.await mainThread)

main1 :: IO ()
main1 = do
  putStrLn "timer wheel runs scheduled actions"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    var <- newEmptyMVar
    let n = 1000 :: Int
    replicateM_ n (register_ wheel 0 (putMVar var ()))
    replicateM_ n (takeMVar var)

  putStrLn "timers can be canceled"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    var <- newEmptyMVar
    let n = 1000 :: Int
    cancels <- replicateM n (register wheel 0 (putMVar var ()))
    successes <- sequence (take (n `div` 2) cancels)
    replicateM_ (n - length (filter id successes)) (takeMVar var)

  putStrLn "successful `cancel` returns true, then false"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    cancel <- register wheel 1 (pure ())
    cancel `is` True
    cancel `is` False

  putStrLn "unsuccessful `cancel` returns false"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    var <- newEmptyMVar
    cancel <- register wheel 0 (putMVar var ())
    takeMVar var
    cancel `is` False

  putStrLn "recurring timers work with delay > resolution work"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    ref <- newIORef (0 :: Int)
    recurring_ wheel 0.2 (modifyIORef' ref (+ 1))
    threadDelay 1_100_000
    readIORef ref `is` (5 :: Int)

  putStrLn "calling `cancel` more than once on a recurring timer is ok"
  with Config {spokes = 4, resolution = 0.05} \wheel -> do
    cancel <- recurring wheel 1 (pure ())
    cancel
    -- At one time, this one would loop indefinitely :grimace:
    cancel

  putStrLn "`with` re-throws exception from background thread"
  ( with Config {spokes = 16, resolution = 0.05} \wheel -> do
      var <- newEmptyMVar
      register_ wheel 0 (throwIO Bye >> putMVar var ())
      takeMVar var
    )
    `is` Exn Bye

  putStrLn "`count` increments on `register`"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    let n = 10 :: Int
    replicateM_ n (register wheel 1 (pure ()))
    count wheel `is` n

  putStrLn "`count` increments on `recurring`"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    let n = 10 :: Int
    replicateM_ n (recurring_ wheel 1 (pure ()))
    count wheel `is` n

  putStrLn "`count` decrements on `cancel` (registered)"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    let n = 10 :: Int
    cancels <- replicateM n (register wheel 1 (pure ()))
    sequence cancels `is` replicate n True
    count wheel `is` (0 :: Int)

  putStrLn "`count` decrements on `cancel` (recurring)"
  with Config {spokes = 16, resolution = 0.05} \wheel -> do
    let n = 10 :: Int
    cancels <- replicateM n (recurring wheel 1 (pure ()))
    sequence_ cancels
    count wheel `is` (0 :: Int)

  putStrLn "stress test: register 1m timers into 1k spokes, then cancel them all"
  with Config {spokes = 1000, resolution = 1} \wheel -> do
    firedRef <- newIORef (0 :: Int)
    let registerLoop :: [IO Bool] -> Random.StdGen -> Int -> IO [IO Bool]
        registerLoop cancels gen0 !i
          | i >= 1_000_000 = pure cancels
          | otherwise = do
              let (delay, gen1) = Random.uniformR (0 :: Double, 10_000) gen0
              cancel <- register wheel (realToFrac @Double @Seconds delay) (modifyIORef' firedRef (+ 1))
              registerLoop (cancel : cancels) gen1 (i + 1)
    let cancelLoop :: Int -> [IO Bool] -> IO Int
        cancelLoop !n = \case
          [] -> pure n
          cancel : cancels -> do
            success <- cancel
            cancelLoop (if success then n + 1 else n) cancels
    cancels <- registerLoop [] (Random.mkStdGen 0) 0
    canceled <- cancelLoop 0 cancels
    fired <- readIORef firedRef
    (fired + canceled) `is` (1_000_000 :: Int)

data Bye = Bye
  deriving stock (Eq, Show)
  deriving anyclass (Exception)

newtype Exn a = Exn a
  deriving stock (Eq)

class Assert a b where
  is :: a -> b -> IO ()

instance (Eq e, Exception e) => Assert (IO void) (Exn e) where
  is mx y = do
    try (void mx) >>= \case
      Left ex
        | Exn ex == y -> pure ()
        | otherwise -> throwIO (userError ("expected different exception, but got: " ++ show ex))
      Right _ -> throwIO (userError "expected exception")

instance (Eq a, Show a) => Assert (IO a) a where
  is mx y = do
    x <- mx
    unless (x == y) (throwIO (userError (show x ++ " /= " ++ show y)))

instance (Eq a, Show a) => Assert a a where
  is x y =
    unless (x == y) (throwIO (userError (show x ++ " /= " ++ show y)))
