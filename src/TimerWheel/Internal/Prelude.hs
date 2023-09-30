module TimerWheel.Internal.Prelude
  ( Pair (..),
    mapPairL,
    mapPairR,
    Seconds,
    module X,
  )
where

import Control.Monad as X (when)
import Data.Coerce as X (coerce)
import Data.Fixed (E6, Fixed)
import Data.IORef as X (IORef, newIORef, readIORef, writeIORef)
import Data.Map as X (Map)
import Data.Word as X (Word64)
import GHC.Generics as X (Generic)
import Prelude as X hiding (lookup, null)

-- A strict pair
data Pair a b
  = Pair !a !b

mapPairL :: (a -> b) -> Pair a x -> Pair b x
mapPairL f (Pair x y) = Pair (f x) y
{-# INLINE mapPairL #-}

mapPairR :: (a -> b) -> Pair x a -> Pair x b
mapPairR f (Pair x y) = Pair x (f y)
{-# INLINE mapPairR #-}

-- | A number of seconds, with microsecond precision.
--
-- You can use numeric literals to construct a value of this type, e.g. @0.5@.
--
-- Otherwise, to convert from a type like @Int@ or @Double@, you can use the generic numeric conversion function
-- @realToFrac@.
type Seconds =
  Fixed E6
