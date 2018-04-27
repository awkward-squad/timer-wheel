{-# language GeneralizedNewtypeDeriving #-}
{-# language ScopedTypeVariables        #-}

module Timestamp where

import Data.Fixed
import System.Clock

newtype Timestamp
  = Timestamp Nano
  deriving (Eq, Ord, Num, Real)

type Duration
  = Timestamp

now :: IO Timestamp
now = do
  time :: TimeSpec <-
    getTime Monotonic
  pure (Timestamp (MkFixed (toNanoSecs time)))

since :: Timestamp -> IO Duration
since t0 =
  subtract t0 <$> now

micro :: Integral a => Duration -> a
micro =
  (`div'` 1000)
