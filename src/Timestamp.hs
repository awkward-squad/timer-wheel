{-# LANGUAGE TypeApplications #-}

module Timestamp
  ( Timestamp (..),
    epoch,
    minus,
    now,
    plus,
    Timestamp.rem,
  )
where

import Data.Coerce (coerce)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Micros (Micros (..))

newtype Timestamp
  = Timestamp Word64
  deriving stock (Eq, Ord)

-- Which epoch does this correspond to, if they are measured in chunks of the given number of milliseconds?
epoch :: Micros -> Timestamp -> Word64
epoch (Micros chunk) (Timestamp timestamp) =
  timestamp `div` chunk

minus :: Timestamp -> Timestamp -> Micros
minus =
  coerce ((-) @Word64)

now :: IO Timestamp
now = do
  nanos <- getMonotonicTimeNSec
  pure (Timestamp (nanos `div` 1000))

plus :: Timestamp -> Micros -> Timestamp
plus =
  coerce ((+) @Word64)

rem :: Timestamp -> Micros -> Micros
rem =
  coerce (Prelude.rem @Word64)
