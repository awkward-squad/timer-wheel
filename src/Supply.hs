{-# language FlexibleContexts #-}

module Supply where

import Data.Atomics.Counter
import Data.Coerce

newtype Supply a
  = Supply AtomicCounter

new :: IO (Supply a)
new =
  Supply <$> newCounter 0

next :: Coercible Int a => Supply a -> IO a
next (Supply counter) =
  coerce <$> incrCounter 1 counter
