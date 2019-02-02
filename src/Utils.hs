{-# LANGUAGE CPP #-}

module Utils
  ( getMonotonicMicros
  ) where

import Data.Word

#if MIN_VERSION_base(4,11,0)
import GHC.Clock (getMonotonicTimeNSec)
#else
import System.Clock (Clock(Monotonic), getTime, toNanoSecs)
#endif

getMonotonicMicros :: IO Word64
getMonotonicMicros =
#if MIN_VERSION_base(4,11,0)
  (`div` 1000) <$> getMonotonicTimeNSec
#else
  ((`div` 1000) . fromIntegral . toNanoSecs <$> getTime Monotonic)
#endif
