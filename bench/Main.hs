import Control.Exception (evaluate)
import Control.Monad (replicateM)
import Data.Coerce (coerce)
import Data.Foldable (for_)
import Data.Word (Word64)
import qualified System.Random as Random
import qualified System.Random.Stateful as Random
import Test.Tasty.Bench
import qualified TimerWheel.Internal.Counter as Counter
import qualified TimerWheel.Internal.Micros as Micros
import qualified TimerWheel.Internal.Timers as Timers

main :: IO ()
main = do
  let delays =
        Random.runStateGen_
          (Random.mkStdGen 0)
          (replicateM 100000 . Random.uniformRM (0 :: Word64, 1000000 * 60 * 5))
  _ <- evaluate (sum delays)

  defaultMain
    [ bench "insert" (whnfIO (insertN (coerce @[Word64] @[Micros.Micros] delays)))
    ]

insertN :: [Micros.Micros] -> IO ()
insertN delays = do
  counter <- Counter.newCounter
  timers <- Timers.create 1024 (Micros.fromSeconds 1)
  for_ delays \delay -> do
    key <- Counter.incrCounter counter
    Timers.insert timers key delay (pure ())
