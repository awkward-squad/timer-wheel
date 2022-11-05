module TimerWheel.Internal.Config
  ( Config (..),
  )
where

import Data.Fixed (E6, Fixed)
import GHC.Generics (Generic)

-- | Timer wheel config.
--
-- * @spokes@ must be ∈ @[1, maxBound]@
-- * @resolution@ must ∈ @(0, ∞]@
data Config = Config
  { -- | Spoke count
    spokes :: {-# UNPACK #-} !Int,
    -- | Resolution, in seconds
    resolution :: !(Fixed E6)
  }
  deriving stock (Generic, Show)
