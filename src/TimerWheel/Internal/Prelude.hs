module TimerWheel.Internal.Prelude
  ( Seconds,
    module X,
  )
where

import Control.Monad as X (when)
import Data.Coerce as X (coerce)
import Data.Fixed (E6, Fixed)
import Data.IORef as X (newIORef, readIORef, writeIORef)
import Data.Word as X (Word64)
import GHC.Generics as X (Generic)

-- | A number of seconds, with microsecond precision.
--
-- You can use numeric literals to construct a value of this type, e.g. @0.5@.
--
-- Otherwise, to convert from a type like @Int@ or @Double@, you can use the generic numeric conversion function
-- @realToFrac@.
type Seconds =
  Fixed E6
