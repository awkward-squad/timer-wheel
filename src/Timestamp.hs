{-# language GeneralizedNewtypeDeriving #-}
{-# language ScopedTypeVariables        #-}

module Timestamp where

import Data.Fixed
import System.Clock

newtype Timestamp
  = Timestamp Nano
  deriving (Eq, Fractional, Ord, Num, Real, RealFrac)

instance Show Timestamp where
  show (Timestamp (MkFixed ns)) =
    show ns

type Duration
  = Timestamp

now :: IO Timestamp
now = do
  time :: TimeSpec <-
    getTime Monotonic
  pure (Timestamp (MkFixed (toNanoSecs time)))

micro :: Duration -> Int
micro =
  floor . (* 1000000)
