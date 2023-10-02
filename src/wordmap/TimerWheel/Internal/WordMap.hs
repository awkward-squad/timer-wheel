-- An IntMap-like container with code cribbed from containers and GHC
--
-- TODO: proper code attribution
module TimerWheel.Internal.WordMap
  ( WordMap (..),

    -- ** Construction
    empty,

    -- ** Querying
    isEmpty,
    lookupExpectingHit,

    -- ** Modification
    insert,
    upsert,
    deleteExpectingHit,
    Pop (..),
    pop,
    splitL,

    -- * Strict pair
    Pair (..),

    -- * Internals
    Mask (..),
    prefixof,
  )
where

import Data.Bits
import Data.Word
import Prelude hiding (lookup, null)

data WordMap a
  = Bin {-# UNPACK #-} !Prefix {-# UNPACK #-} !Mask !(WordMap1 a) !(WordMap1 a)
  | Tip {-# UNPACK #-} !Word64 !a
  | Nil

-- A non-empty wordmap.
type WordMap1 a = WordMap a

type Prefix = Word64

newtype Mask = Mask Word64

empty :: WordMap a
empty =
  Nil
{-# INLINE empty #-}

isEmpty :: WordMap a -> Bool
isEmpty = \case
  Nil -> True
  _ -> False
{-# INLINE isEmpty #-}

-- | Look up an element in a wordmap, expecting it to be there.
lookupExpectingHit :: forall a. Word64 -> WordMap a -> Maybe a
lookupExpectingHit !k =
  go
  where
    go :: WordMap a -> Maybe a
    go = \case
      -- Ignore prefix because we expect to find the key
      Bin _ m l r -> go (if leftside k m then l else r)
      Tip kx x -> if k == kx then Just x else Nothing
      Nil -> Nothing

-- | Delete an element from a wordmap, expecting it to be there.
deleteExpectingHit :: Word64 -> WordMap a -> WordMap a
deleteExpectingHit !k t =
  case t of
    Bin p m l r
      -- Ignore prefix because we expect to find the key
      | leftside k m -> binl p m (deleteExpectingHit k l) r
      | otherwise -> binr p m l (deleteExpectingHit k r)
    Tip kx _
      | k == kx -> Nil
      | otherwise -> t
    Nil -> Nil

insert :: Word64 -> a -> WordMap a -> WordMap1 a
insert !k !x t =
  case t of
    Bin p m l r
      | outsider k p m -> link k (Tip k x) p t
      | leftside k m -> Bin p m (insert k x l) r
      | otherwise -> Bin p m l (insert k x r)
    Tip ky _
      | k == ky -> Tip k x
      | otherwise -> link k (Tip k x) ky t
    Nil -> Tip k x

upsert :: Word64 -> a -> (a -> a) -> WordMap a -> WordMap a
upsert !k !x f t =
  case t of
    Bin p m l r
      | outsider k p m -> link k (Tip k x) p t
      | leftside k m -> Bin p m (upsert k x f l) r
      | otherwise -> Bin p m l (upsert k x f r)
    Tip ky y
      | k == ky -> Tip k (f y)
      | otherwise -> link k (Tip k x) ky t
    Nil -> Tip k x

data Pop a
  = PopNada
  | PopAlgo !Word64 !a !(WordMap a)

pop :: WordMap a -> Pop a
pop = \case
  Nil -> PopNada
  t -> pop1 t
{-# INLINE pop #-}

pop1 :: WordMap1 a -> Pop a
pop1 = \case
  Bin p m l r ->
    case pop1 l of
      PopAlgo k x l1 -> PopAlgo k x (binl p m l1 r)
      PopNada -> undefined
  Tip k x -> PopAlgo k x Nil
  Nil -> undefined

-- @splitL k xs@ splits @xs@ into @ys@ and @zs@, where every value in @ys@ is less than or equal to @k@, and every value
-- in @zs@ is greater than @k@.
splitL :: forall a. Word64 -> WordMap a -> Pair (WordMap a) (WordMap a)
splitL k =
  go
  where
    go :: WordMap a -> Pair (WordMap a) (WordMap a)
    go t =
      case t of
        Bin p m l r
          | outsider k p m ->
              if k > p
                then Pair t Nil
                else Pair Nil t
          | leftside k m -> mapPairR (\lr -> bin p m lr r) (go l)
          | otherwise -> mapPairL (bin p m l) (go r)
        Tip k2 _
          | k >= k2 -> Pair t Nil
          | otherwise -> Pair Nil t
        Nil -> Pair Nil Nil

------------------------------------------------------------------------------------------------------------------------
-- Low-level constructors

-- | Combine two wordmaps with a Bin constructor if they are both non-empty. Otherwise, just return one that's not known
-- to be empty.
bin :: Prefix -> Mask -> WordMap a -> WordMap a -> WordMap a
bin _ _ l Nil = l
bin _ _ Nil r = r
bin p m l r = Bin p m l r
{-# INLINE bin #-}

-- | Like 'bin', but for when the right wordmap is known to be non-empty.
binl :: Prefix -> Mask -> WordMap a -> WordMap1 a -> WordMap1 a
binl _ _ Nil r = r
binl p m l r = Bin p m l r
{-# INLINE binl #-}

-- | Like 'bin', but for when the left wordmap is known to be non-empty.
binr :: Prefix -> Mask -> WordMap1 a -> WordMap a -> WordMap1 a
binr _ _ l Nil = l
binr p m l r = Bin p m l r
{-# INLINE binr #-}

link :: Prefix -> WordMap a -> Prefix -> WordMap a -> WordMap a
link p1 t1 p2 t2
  | leftside p1 m = Bin p m t1 t2
  | otherwise = Bin p m t2 t1
  where
    m = highestBitMask (p1 `xor` p2)
    p = prefixof p1 m
{-# INLINE link #-}

------------------------------------------------------------------------------------------------------------------------
-- Bit twiddling

-- @leftside w m@ returns whether key @k@ belongs of the left side of a bin with mask @m@.
--
-- Precondition: @w@ is not an "outsider".
leftside :: Word64 -> Mask -> Bool
leftside w (Mask m) =
  w .&. m == 0
{-# INLINE leftside #-}

-- @outsider w p m@ returns whether @w@ is an "outsider" of a bin with prefix @p@ and mask @m@; that is, it is known
-- not to be on either side.
outsider :: Word64 -> Prefix -> Mask -> Bool
outsider w p m =
  prefixof w m /= p
{-# INLINE outsider #-}

-- As you can see from this:
--
--        m = 00000000 00000000 00000000 00010000 00000000 00000000 00000000 00000000
--     -m   = 11111111 11111111 11111111 11110000 00000000 00000000 00000000 00000000
-- xor -m m = 11111111 11111111 11111111 11100000 00000000 00000000 00000000 00000000
--
-- @prefixof w m@ retains only the prefix bits of word @w@, per mask @m@.
prefixof :: Word64 -> Mask -> Prefix
prefixof w (Mask m) =
  w .&. ((-m) `xor` m)
{-# INLINE prefixof #-}

-- | Return a word where only the highest bit is set.
highestBitMask :: Word64 -> Mask
highestBitMask w = Mask (unsafeShiftL 1 (63 - countLeadingZeros w))
{-# INLINE highestBitMask #-}

------------------------------------------------------------------------------------------------------------------------
-- Strict pair stuff

data Pair a b
  = Pair !a !b

mapPairL :: (a -> b) -> Pair a x -> Pair b x
mapPairL f (Pair x y) = Pair (f x) y
{-# INLINE mapPairL #-}

mapPairR :: (a -> b) -> Pair x a -> Pair x b
mapPairR f (Pair x y) = Pair x (f y)
{-# INLINE mapPairR #-}
