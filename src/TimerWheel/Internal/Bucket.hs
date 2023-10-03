module TimerWheel.Internal.Bucket
  ( Bucket,
    empty,

    -- * Queries
    isEmpty,
    partition,

    -- * Modifications
    insert,
    Pop (..),
    pop,
    deleteExpectingHit,

    -- * Strict pair
    Pair (..),
  )
where

import Data.Bits
import TimerWheel.Internal.Prelude
import TimerWheel.Internal.Timestamp (Timestamp)

data Bucket a
  = -- Invariants on `Bin k p v m l r`:
    --   1. `l` and `r` can't both be Nil
    --   2. `p` is <= all `p` in `l` and `r`
    --   3. `k` is not an element of `l` nor `r`
    --   4. `m` has one 1-bit, which is the highest bit position at which any two keys in `l` and `r` differ
    --   5. No key in `l` has the `m` bit set
    --   6. All keys in `r` have the `m` bit set
    Bin {-# UNPACK #-} !TimerId {-# UNPACK #-} !Timestamp !a {-# UNPACK #-} !Mask !(Bucket a) !(Bucket a)
  | Tip {-# UNPACK #-} !TimerId {-# UNPACK #-} !Timestamp !a
  | Nil

type Mask = Word64

type TimerId = Int

-- | An empty bucket.
empty :: Bucket a
empty =
  Nil

isEmpty :: Bucket a -> Bool
isEmpty = \case
  Nil -> True
  _ -> False

-- | Partition a bucket by timestamp (less-than-or-equal-to, greater-than).
partition :: forall a. Timestamp -> Bucket a -> Pair (Bucket a) (Bucket a)
partition q =
  go empty
  where
    go :: Bucket a -> Bucket a -> Pair (Bucket a) (Bucket a)
    go acc t =
      case t of
        Nil -> Pair acc t
        Tip i p x
          | p > q -> Pair acc t
          | otherwise -> Pair (insert i p x acc) Nil
        Bin i p x m l r
          | p > q -> Pair acc t
          | otherwise ->
              case go acc l of
                Pair acc1 l1 ->
                  case go acc1 r of
                    Pair acc2 r1 -> Pair (insert i p x acc2) (merge m l1 r1)

-- | Insert a new timer into a bucket.
--
-- If a timer with the given id is already in the bucket, behavior is undefined.
insert :: forall a. TimerId -> Timestamp -> a -> Bucket a -> Bucket a
insert i p x t =
  case t of
    Nil -> Tip i p x
    Tip j q y
      | (p, i) < (q, j) -> link i p x j t Nil
      | otherwise -> link j q y i (Tip i p x) Nil
    Bin j q y m l r
      | prefixNotEqual m i j ->
          if (p, i) < (q, j)
            then link i p x j t Nil
            else link j q y i (Tip i p x) (merge m l r)
      | (p, i) < (q, j) ->
          if goleft j m
            then Bin i p x m (insert j q y l) r
            else Bin i p x m l (insert j q y r)
      | otherwise ->
          if goleft i m
            then Bin j q y m (insert i p x l) r
            else Bin j q y m l (insert i p x r)

data Pop a
  = PopAlgo !TimerId !Timestamp !a !(Bucket a)
  | PopNada

pop :: Bucket a -> Pop a
pop = \case
  Nil -> PopNada
  Tip k p x -> PopAlgo k p x Nil
  Bin k p x m l r -> PopAlgo k p x (merge m l r)
{-# INLINE pop #-}

-- | Delete a timer from a bucket, expecting it to be there.
deleteExpectingHit :: TimerId -> Bucket v -> Maybe (Bucket v)
deleteExpectingHit i =
  go
  where
    go :: Bucket v -> Maybe (Bucket v)
    go = \case
      Nil -> Nothing
      Tip j _ _
        | i == j -> Just Nil
        | otherwise -> Nothing
      Bin j p x m l r
        -- This commented out short-circuit is what makes this delete variant "expecting a hit"
        --   | nomatch m i j -> Nothing
        | i == j -> Just $! merge m l r
        | goleft i m -> (\l1 -> bin j p x m l1 r) <$> go l
        | otherwise -> bin j p x m l <$> go r

i2w :: TimerId -> Word64
i2w = fromIntegral
{-# INLINE i2w #-}

goleft :: TimerId -> Mask -> Bool
goleft i m =
  i2w i .&. m == 0
{-# INLINE goleft #-}

-- m = 00001000000000000000000
-- i = IIII???????????????????
-- j = JJJJ???????????????????
--
-- prefixNotEqual m i j answers, is IIII not equal to JJJJ?
prefixNotEqual :: Mask -> TimerId -> TimerId -> Bool
prefixNotEqual (prefixMask -> e) i j =
  i2w i .&. e /= i2w j .&. e
{-# INLINE prefixNotEqual #-}

--            m = 0000000000100000
-- prefixMask m = 1111111111000000
prefixMask :: Word64 -> Word64
prefixMask m = -m `xor` m
{-# INLINE prefixMask #-}

onlyHighestBit :: Word64 -> Mask
onlyHighestBit w = unsafeShiftL 1 (63 - countLeadingZeros w)
{-# INLINE onlyHighestBit #-}

link :: TimerId -> Timestamp -> v -> TimerId -> Bucket v -> Bucket v -> Bucket v
link i p x j l r
  | goleft j m = Bin i p x m l r
  | otherwise = Bin i p x m r l
  where
    m = onlyHighestBit (i2w i `xor` i2w j)

-- | 'Bin' smart constructor, respecting the invariant that both children can't be 'Nil'.
bin :: TimerId -> Timestamp -> v -> Mask -> Bucket v -> Bucket v -> Bucket v
bin i p x _ Nil Nil = Tip i p x
bin i p x m l r = Bin i p x m l r
{-# INLINE bin #-}

-- Merge two disjoint buckets that have the same mask.
merge :: Mask -> Bucket v -> Bucket v -> Bucket v
merge m l r =
  case (l, r) of
    (Nil, _) -> r
    (_, Nil) -> l
    --
    --    ip      jq
    --
    (Tip i p x, Tip j q y)
      --
      --       ip
      --      /  \
      --    nil  jq
      --
      | (p, i) < (q, j) -> Bin i p x m Nil r
      --
      --       jq
      --      /  \
      --    ip   nil
      --
      | otherwise -> Bin j q y m l Nil
    --
    --    ip      jq
    --           /  \
    --         rl    rr
    --
    (Tip i p x, Bin j q y n rl rr)
      --
      --       ip
      --      /  \
      --    nil  jq
      --        /  \
      --      rl    rr
      --
      | (p, i) < (q, j) -> Bin i p x m Nil r
      --
      --       jq
      --      /  \
      --    ip   rl+rr
      --
      | otherwise -> Bin j q y m l (merge n rl rr)
    --
    --       ip      jq
    --      /  \
    --    ll    lr
    --
    (Bin i p x n ll lr, Tip j q y)
      --
      --         ip
      --        /  \
      --    ll+lr   jq
      --
      | (p, i) < (q, j) -> Bin i p x m (merge n ll lr) r
      --
      --          jq
      --         /  \
      --       ip   nil
      --      /  \
      --    ll    lr
      --
      | otherwise -> Bin j q y m l Nil
    --
    --       ip          jq
    --      /  \        /  \
    --    ll    lr    rl    rr
    --
    (Bin i p x n ll lr, Bin j q y o rl rr)
      --
      --         ip
      --        /  \
      --    ll+lr   jq
      --           /  \
      --         rl    rr
      --
      | (p, i) < (q, j) -> Bin i p x m (merge n ll lr) r
      --
      --          jq
      --         /  \
      --       ip   rl+rr
      --      /  \
      --    ll    lr
      --
      | otherwise -> Bin j q y m l (merge o rl rr)

data Pair a b
  = Pair !a !b
