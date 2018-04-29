{-# language FlexibleContexts #-}

module Supply where

import Data.Atomics.Counter (AtomicCounter, incrCounter, newCounter)
import Data.Coerce (Coercible, coerce)

newtype Supply a
  = Supply AtomicCounter

new :: IO (Supply a)
new =
  Supply <$> newCounter 0

next :: Coercible Int a => Supply a -> IO a
next (Supply counter) =
  coerce <$> incrCounter 1 counter
{-# INLINABLE next #-}
