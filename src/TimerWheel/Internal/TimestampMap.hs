-- A Word64Map with code cribbed from containers and GHC
-- TODO: proper code attribution
module TimerWheel.Internal.TimestampMap
  ( TimestampMap,
    delete,
    empty,
    foreach,
    insert,
    lookup,
    null,
    splitL,
    upsert,
  )
where

import Data.Bits
import Data.Word
import TimerWheel.Internal.Nanoseconds (Nanoseconds (..))
import TimerWheel.Internal.Prelude
import TimerWheel.Internal.Timestamp (Timestamp (..))

data TimestampMap a
  = Bin
      {-# UNPACK #-} !Prefix
      {-# UNPACK #-} !Mask
      !(TimestampMap a)
      !(TimestampMap a)
  | Tip {-# UNPACK #-} !Word64 a
  | Nil

type Prefix = Word64

type Mask = Word64

bin :: Prefix -> Mask -> TimestampMap a -> TimestampMap a -> TimestampMap a
bin _ _ l Nil = l
bin _ _ Nil r = r
bin p m l r = Bin p m l r
{-# INLINE bin #-}

binCheckLeft :: Prefix -> Mask -> TimestampMap a -> TimestampMap a -> TimestampMap a
binCheckLeft _ _ Nil r = r
binCheckLeft p m l r = Bin p m l r
{-# INLINE binCheckLeft #-}

binCheckRight :: Prefix -> Mask -> TimestampMap a -> TimestampMap a -> TimestampMap a
binCheckRight _ _ l Nil = l
binCheckRight p m l r = Bin p m l r
{-# INLINE binCheckRight #-}

delete :: forall a. Timestamp -> TimestampMap a -> TimestampMap a
delete = coerce @(Word64 -> TimestampMap a -> TimestampMap a) delete_
{-# INLINE delete #-}

delete_ :: Word64 -> TimestampMap a -> TimestampMap a
delete_ !k = \case
  t@(Bin p m l r)
    | nomatch k p m -> t
    | zero k m -> binCheckLeft p m (delete_ k l) r
    | otherwise -> binCheckRight p m l (delete_ k r)
  t@(Tip kx _)
    | k == kx -> Nil
    | otherwise -> t
  Nil -> Nil

empty :: TimestampMap a
empty =
  Nil
{-# INLINE empty #-}

foreach :: (Timestamp -> a -> IO ()) -> TimestampMap a -> IO ()
foreach f =
  go
  where
    go = \case
      Nil -> pure ()
      Tip k x -> f (coerce @Word64 @Timestamp k) x
      Bin _ _ l r -> go l >> go r

insert :: forall a. Timestamp -> a -> TimestampMap a -> TimestampMap a
insert = coerce @(Word64 -> a -> TimestampMap a -> TimestampMap a) insert_
{-# INLINE insert #-}

insert_ :: Word64 -> a -> TimestampMap a -> TimestampMap a
insert_ !k !x t =
  case t of
    Bin p m l r
      | nomatch k p m -> link k (Tip k x) p t
      | zero k m -> Bin p m (insert_ k x l) r
      | otherwise -> Bin p m l (insert_ k x r)
    Tip ky _
      | k == ky -> Tip k x
      | otherwise -> link k (Tip k x) ky t
    Nil -> Tip k x

link :: Prefix -> TimestampMap a -> Prefix -> TimestampMap a -> TimestampMap a
link p1 t1 p2 = linkWithMask (branchMask p1 p2) p1 t1
{-# INLINE link #-}

-- `linkWithMask` is useful when the `branchMask` has already been computed
linkWithMask :: Mask -> Prefix -> TimestampMap a -> TimestampMap a -> TimestampMap a
linkWithMask m p0 t1 t2
  | zero p0 m = Bin p m t1 t2
  | otherwise = Bin p m t2 t1
  where
    p = mask p0 m
{-# INLINE linkWithMask #-}

lookup :: forall a. Timestamp -> TimestampMap a -> Maybe a
lookup = coerce @(Word64 -> TimestampMap a -> Maybe a) lookup_
{-# INLINE lookup #-}

lookup_ :: forall a. Word64 -> TimestampMap a -> Maybe a
lookup_ !k =
  go
  where
    go :: TimestampMap a -> Maybe a
    go = \case
      Bin _ m l r -> go (if zero k m then l else r)
      Tip kx x -> if k == kx then Just x else Nothing
      Nil -> Nothing

null :: TimestampMap a -> Bool
null = \case
  Nil -> True
  _ -> False
{-# INLINE null #-}

singleton :: Word64 -> a -> TimestampMap a
singleton k !x = Tip k x
{-# INLINE singleton #-}

-- @splitL k xs@ splits @xs@ into @ys@ and @zs@, where every timestamp in @ys@ is less than or equal to @k@, and every
-- timestamp in @zs@ is greater than @k@.
splitL :: forall a. Timestamp -> TimestampMap a -> Pair (TimestampMap a) (TimestampMap a)
splitL = coerce @(Word64 -> TimestampMap a -> Pair (TimestampMap a) (TimestampMap a)) splitL_
{-# INLINE splitL #-}

splitL_ :: forall a. Word64 -> TimestampMap a -> Pair (TimestampMap a) (TimestampMap a)
splitL_ k = \case
  Bin p m l r | m < 0 -> mapPairL (\ll -> bin p m ll r) (go l)
  t -> go t
  where
    go :: TimestampMap a -> Pair (TimestampMap a) (TimestampMap a)
    go = \case
      t@(Bin p m l r)
        | nomatch k p m ->
            if k > p
              then Pair t Nil
              else Pair Nil t
        | zero k m -> mapPairR (\lr -> bin p m lr r) (go l)
        | otherwise -> mapPairL (\rl -> bin p m l rl) (go r)
      t@(Tip k2 _)
        | k >= k2 -> Pair t Nil
        | otherwise -> Pair Nil t
      Nil -> Pair Nil Nil

upsert :: forall a. Timestamp -> a -> (a -> a) -> TimestampMap a -> TimestampMap a
upsert = coerce @(Word64 -> a -> (a -> a) -> TimestampMap a -> TimestampMap a) upsert_
{-# INLINE upsert #-}

upsert_ :: Word64 -> a -> (a -> a) -> TimestampMap a -> TimestampMap a
upsert_ !k x f = \case
  t@(Bin p m l r)
    | nomatch k p m -> link k (singleton k x) p t
    | zero k m -> Bin p m (upsert_ k x f l) r
    | otherwise -> Bin p m l (upsert_ k x f r)
  t@(Tip ky y)
    | k == ky -> Tip k $! f y
    | otherwise -> link k (singleton k x) ky t
  Nil -> singleton k x

------------------------------------------------------------------------------------------------------------------------
-- Bit twiddling

-- | Should this key follow the left subtree of a 'Bin' with switching bit @m@?
-- N.B., the answer is only valid when @match i p m@ is true.
zero :: Word64 -> Mask -> Bool
zero i m =
  i .&. m == 0
{-# INLINE zero #-}

-- | Does the key @i@ differ from the prefix @p@ before getting to the switching bit @m@?
nomatch :: Word64 -> Prefix -> Mask -> Bool
nomatch i p m =
  mask i m /= p
{-# INLINE nomatch #-}

-- | The prefix of key @i@ up to (but not including) the switching bit @m@.
mask :: Word64 -> Mask -> Prefix
mask i m =
  i .&. ((-m) `xor` m)
{-# INLINE mask #-}

-- | The first switching bit where the two prefixes disagree.
branchMask :: Prefix -> Prefix -> Mask
branchMask p1 p2 =
  highestBitMask (p1 `xor` p2)
{-# INLINE branchMask #-}

-- | Return a word where only the highest bit is set.
highestBitMask :: Word64 -> Word64
highestBitMask w = unsafeShiftL 1 (63 - countLeadingZeros w)
{-# INLINE highestBitMask #-}
