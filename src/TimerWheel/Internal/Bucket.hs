module TimerWheel.Internal.Bucket
  ( Bucket,
    empty,
    isEmpty,
    partition,
    insert,
    Pop (..),
    pop,
    delete,
  )
where

import Data.IntPSQ (IntPSQ)
import qualified Data.IntPSQ as IntPSQ
import TimerWheel.Internal.Prelude

type Bucket a =
  IntPSQ Word64 a

type TimerId = Int

type Timestamp = Word64

-- | An empty bucket.
empty :: Bucket a
empty =
  IntPSQ.empty

isEmpty :: Bucket a -> Bool
isEmpty =
  IntPSQ.null

partition :: Timestamp -> Bucket a -> (Bucket a, Bucket a)
partition timestamp bucket =
  let (expired0, alive) = IntPSQ.atMostView timestamp bucket
      !expired1 = IntPSQ.fromList expired0
   in (expired1, alive)

-- | Insert a new timer into a bucket.
insert :: TimerId -> Timestamp -> a -> Bucket a -> Bucket a
insert =
  IntPSQ.unsafeInsertNew

data Pop a
  = PopAlgo !TimerId !Timestamp !a !(Bucket a)
  | PopNada

pop :: Bucket a -> Pop a
pop =
  maybe PopNada (\(timerId, timestamp, value, bucket) -> PopAlgo timerId timestamp value bucket) . IntPSQ.minView

delete :: TimerId -> Bucket a -> Maybe (Bucket a)
delete timerId =
  fmap (\(_, _, bucket) -> bucket) . IntPSQ.deleteView timerId
