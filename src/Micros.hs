{-# LANGUAGE TypeApplications #-}

module Micros
  ( Micros (..),
    fromFixed,
    Micros.div,
    minus,
    scale,
    sleep,
  )
where

import Control.Concurrent (threadDelay)
import Data.Coerce
import Data.Fixed
import Data.Word

newtype Micros = Micros {unMicros :: Word64}
  deriving stock (Eq, Ord)

-- | Precondition: input is >= 0
fromFixed :: Fixed E6 -> Micros
fromFixed =
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
