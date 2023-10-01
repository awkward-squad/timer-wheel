module TimerWheel.Internal.Prelude
  ( Seconds,
    module X,
  )
where

import Control.Monad as X (when)
import Data.Coerce as X (coerce)
import Data.Fixed (E9, Fixed)
import Data.IORef as X (IORef, newIORef, readIORef, writeIORef)
import Data.Word as X (Word64)
import GHC.Generics as X (Generic)
import Prelude as X hiding (lookup, null)

-- | A number of seconds, with nanosecond precision.
--
-- You can use numeric literals to construct a value of this type, e.g. @0.5@.
--
-- Otherwise, to convert from a type like @Int@ or @Double@, you can use the generic numeric conversion function
-- @realToFrac@.
type Seconds =
  Fixed E9
