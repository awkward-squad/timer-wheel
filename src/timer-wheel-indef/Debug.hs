{-# language CPP #-}

module Debug
  ( debug
  ) where

import Control.Concurrent
import System.IO.Unsafe

#ifdef DEBUG
iolock :: MVar ()
iolock =
  unsafePerformIO (newMVar ())
{-# NOINLINE iolock #-}
#endif

debug :: IO () -> IO ()
#ifdef DEBUG
debug action =
  withMVar iolock (\_ -> action)
#else
debug _ =
  pure ()
#endif
{-# INLINE debug #-}
