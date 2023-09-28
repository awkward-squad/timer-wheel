module TimerWheel.Internal.Micros
  ( Micros (..),
    fromSeconds,
    fromNonNegativeSeconds,
    TimerWheel.Internal.Micros.div,
    minus,
    scale,
    sleep,
  )
where

import Control.Concurrent (threadDelay)
import Data.Fixed (Fixed (..))
import TimerWheel.Internal.Prelude

newtype Micros = Micros {unMicros :: Word64}
  deriving stock (Eq, Ord)

-- | Convert a number of seconds into a number of microseconds.
--
-- Negative values are converted to 0.
fromSeconds :: Seconds -> Micros
fromSeconds =
  fromNonNegativeSeconds . max 0

-- | Like 'fromNonNegativeSeconds', but with an unchecked precondition: the given seconds is non-negative.
--
-- What you get for your troubles: one puny fewer int comparisons.
fromNonNegativeSeconds :: Seconds -> Micros
fromNonNegativeSeconds =
  coerce @(Integer -> Word64) fromIntegral

div :: Micros -> Micros -> Micros
div =
  coerce (Prelude.div @Word64)

minus :: Micros -> Micros -> Micros
minus =
  coerce ((-) @Word64)

scale :: Int -> Micros -> Micros
scale n (Micros w) =
  Micros (fromIntegral n * w)

sleep :: Micros -> IO ()
sleep (Micros micros) =
  threadDelay (fromIntegral micros)
