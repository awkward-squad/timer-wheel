-- An IntMap-like container with code cribbed from containers and GHC
--
-- TODO: proper code attribution
module TimerWheel.Internal.WordMap
  ( WordMap (..),
    delete,
    empty,
    insert,
    lookup,
    null,
    Pop (..),
    pop,
    splitL,
    upsert,

    -- * Strict pair
    Pair (..),

    -- * Internals
    mask,
  )
where

import Data.Bits
import Data.Word
import Prelude hiding (lookup, null)

data WordMap a
  = Bin {-# UNPACK #-} !Prefix {-# UNPACK #-} !Mask !(WordMap1 a) !(WordMap1 a)
  | Tip {-# UNPACK #-} !Word64 a
  | Nil

-- A non-empty word map.
type WordMap1 a = WordMap a

type Prefix = Word64

type Mask = Word64

bin :: Prefix -> Mask -> WordMap a -> WordMap a -> WordMap a
bin _ _ l Nil = l
bin _ _ Nil r = r
bin p m l r = Bin p m l r
{-# INLINE bin #-}

binCheckLeft :: Prefix -> Mask -> WordMap a -> WordMap1 a -> WordMap1 a
binCheckLeft _ _ Nil r = r
binCheckLeft p m l r = Bin p m l r
{-# INLINE binCheckLeft #-}

binCheckRight :: Prefix -> Mask -> WordMap1 a -> WordMap a -> WordMap1 a
binCheckRight _ _ l Nil = l
binCheckRight p m l r = Bin p m l r
{-# INLINE binCheckRight #-}

delete :: Word64 -> WordMap a -> WordMap a
delete !k t =
  case t of
    Bin p m l r
      | nomatch k p m -> t
      | zero k m -> binCheckLeft p m (delete k l) r
      | otherwise -> binCheckRight p m l (delete k r)
    Tip kx _
      | k == kx -> Nil
      | otherwise -> t
    Nil -> Nil

empty :: WordMap a
empty =
  Nil
{-# INLINE empty #-}

insert :: Word64 -> a -> WordMap a -> WordMap a
insert !k !x t =
  case t of
    Bin p m l r
      | nomatch k p m -> link k (Tip k x) p t
      | zero k m -> Bin p m (insert k x l) r
      | otherwise -> Bin p m l (insert k x r)
    Tip ky _
      | k == ky -> Tip k x
      | otherwise -> link k (Tip k x) ky t
    Nil -> Tip k x

link :: Prefix -> WordMap a -> Prefix -> WordMap a -> WordMap a
link p1 t1 p2 = linkWithMask (branchMask p1 p2) p1 t1
{-# INLINE link #-}

-- `linkWithMask` is useful when the `branchMask` has already been computed
linkWithMask :: Mask -> Prefix -> WordMap a -> WordMap a -> WordMap a
linkWithMask m p0 t1 t2
  | zero p0 m = Bin p m t1 t2
  | otherwise = Bin p m t2 t1
  where
    p = mask p0 m
{-# INLINE linkWithMask #-}

lookup :: forall a. Word64 -> WordMap a -> Maybe a
lookup !k =
  go
  where
    go :: WordMap a -> Maybe a
    go = \case
      Bin _ m l r -> go (if zero k m then l else r)
      Tip kx x -> if k == kx then Just x else Nothing
      Nil -> Nothing

null :: WordMap a -> Bool
null = \case
  Nil -> True
  _ -> False
{-# INLINE null #-}

singleton :: Word64 -> a -> WordMap a
singleton k !x = Tip k x
{-# INLINE singleton #-}

-- @splitL k xs@ splits @xs@ into @ys@ and @zs@, where every value in @ys@ is less than or equal to @k@, and every value
-- in @zs@ is greater than @k@.
splitL :: forall a. Word64 -> WordMap a -> Pair (WordMap a) (WordMap a)
splitL k =
  go
  where
    go :: WordMap a -> Pair (WordMap a) (WordMap a)
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

data Pop a
  = PopNada
  | PopAlgo !Word64 !a !(WordMap a)

pop :: WordMap a -> Pop a
pop = \case
  Nil -> PopNada
  Bin p m l r0 | m < 0 ->
    case pop1 r0 of
      PopAlgo k x r1 -> PopAlgo k x (binCheckRight p m l r1)
      PopNada -> undefined
  t -> pop1 t
{-# INLINE pop #-}

pop1 :: WordMap1 a -> Pop a
pop1 = \case
  Bin p m l0 r ->
    case pop1 l0 of
      PopAlgo k x l1 -> PopAlgo k x (binCheckLeft p m l1 r)
      PopNada -> undefined
  Tip k x -> PopAlgo k x Nil
  Nil -> undefined

upsert :: Word64 -> a -> (a -> a) -> WordMap a -> WordMap a
upsert !k x f = \case
  t@(Bin p m l r)
    | nomatch k p m -> link k (singleton k x) p t
    | zero k m -> Bin p m (upsert k x f l) r
    | otherwise -> Bin p m l (upsert k x f r)
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

-- A strict pair
data Pair a b
  = Pair !a !b

mapPairL :: (a -> b) -> Pair a x -> Pair b x
mapPairL f (Pair x y) = Pair (f x) y
{-# INLINE mapPairL #-}

mapPairR :: (a -> b) -> Pair x a -> Pair x b
mapPairR f (Pair x y) = Pair x (f y)
{-# INLINE mapPairR #-}
