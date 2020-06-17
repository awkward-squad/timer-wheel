module Supply
  ( Supply,
    new,
    next,
  )
where

import Data.Atomics.Counter (AtomicCounter, incrCounter, newCounter)

newtype Supply
  = Supply AtomicCounter

new :: IO Supply
new =
  Supply <$> newCounter 0

next :: Supply -> IO Int
next (Supply counter) =
  incrCounter 1 counter
{-# INLINE next #-}
