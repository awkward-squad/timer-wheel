import Control.Exception (evaluate)
import Control.Monad (replicateM)
import Data.Foldable (for_)
import GHC.Conc (atomically)
import qualified Ki
import Prelude
import qualified System.Random as Random
import qualified System.Random.Stateful as Random
import Test.Tasty.Bench
import qualified TimerWheel

main :: IO ()
main = do
  Ki.scoped \scope -> do
    mainThread <- Ki.fork scope main1
    atomically (Ki.await mainThread)

main1 :: IO ()
main1 = do
  let delays :: [Double]
      delays =
        Random.runStateGen_
          (Random.mkStdGen 0)
          (replicateM 1_000_000 . Random.uniformRM (0, 60 * 5))
  _ <- evaluate (sum delays)

  defaultMain
    [ bench "insert" (whnfIO (insertN (map (realToFrac @Double @TimerWheel.Seconds) delays)))
    ]

insertN :: [TimerWheel.Seconds] -> IO ()
insertN delays =
  TimerWheel.with TimerWheel.Config {spokes = 1024, resolution = 1} \wheel -> do
    for_ delays \delay -> do
      TimerWheel.register wheel delay (pure ())
