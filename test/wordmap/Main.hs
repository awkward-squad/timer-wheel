module Main (main) where

import Data.Bits (popCount, (.&.))
import Data.Foldable (for_)
import qualified Data.List as List
import qualified Data.Set as Set
import Data.Word
import GHC.Conc (atomically)
import Hedgehog ((===))
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Main as Hedgehog
import qualified Hedgehog.Range as Range
import qualified Ki
import TimerWheel.Internal.WordMap (WordMap)
import qualified TimerWheel.Internal.WordMap as WordMap
import Prelude

main :: IO ()
main = do
  Ki.scoped \scope -> do
    mainThread <- Ki.fork scope main1
    atomically (Ki.await mainThread)

main1 :: IO ()
main1 =
  Hedgehog.defaultMain [Hedgehog.checkParallel (Hedgehog.Group "Wordmap tests" tests)]

tests :: [(Hedgehog.PropertyName, Hedgehog.Property)]
tests =
  [ ( "insert lookup",
      Hedgehog.property do
        keys <- Hedgehog.forAll (Gen.list (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        let m = listToWordMap (map (,()) keys)
        for_ keys \key ->
          WordMap.lookup key m === Just ()
    ),
    ( "insert pop",
      Hedgehog.property do
        keys <- Set.toList <$> Hedgehog.forAll (Gen.set (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        keys === wordMapKeysList (listToWordMap (map (,()) keys))
    ),
    ( "insert splitL",
      Hedgehog.property do
        keys <- Hedgehog.forAll (Gen.list (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        key <- Hedgehog.forAll (Gen.word64 Range.linearBounded)
        let WordMap.Pair xs ys = WordMap.splitL key (listToWordMap (map (,()) keys))
        Hedgehog.assert (all (<= key) (wordMapKeysList xs))
        Hedgehog.assert (all (> key) (wordMapKeysList ys))
    ),
    ( "valid internal structure",
      Hedgehog.property do
        let loop :: (Hedgehog.MonadTest m) => WordMap a -> [Command a] -> m ()
            loop m = \case
              [] -> pure ()
              command : commands -> do
                let m1 = applyCommand command m
                Hedgehog.assert (wordMapIsValid m1)
                loop m1 commands
        commands <-
          Hedgehog.forAll do
            Gen.list
              (Range.linear 1 1000)
              ( Gen.frequency
                  [ (10, Insert <$> Gen.word64 Range.linearBounded <*> Gen.word8 Range.linearBounded),
                    (10, Delete <$> Gen.word64 Range.linearBounded),
                    (10, pure Pop),
                    (10, KeepL <$> Gen.word64 Range.linearBounded),
                    (10, KeepR <$> Gen.word64 Range.linearBounded)
                  ]
              )
        loop WordMap.empty commands
    )
  ]

data Command a
  = Delete !Word64
  | Insert !Word64 !a
  | KeepL !Word64
  | KeepR !Word64
  | Pop
  deriving stock (Show)

applyCommand :: Command a -> WordMap a -> WordMap a
applyCommand = \case
  Delete k -> WordMap.delete k
  Insert k v -> WordMap.insert k v
  KeepL k -> \m -> case WordMap.splitL k m of WordMap.Pair x _ -> x
  KeepR k -> \m -> case WordMap.splitL k m of WordMap.Pair _ x -> x
  Pop -> \m -> case WordMap.pop m of WordMap.PopNada -> m; WordMap.PopAlgo _ _ m1 -> m1

------------------------------------------------------------------------------------------------------------------------
-- Random utils

wordMapIsValid :: WordMap a -> Bool
wordMapIsValid = \case
  WordMap.Bin p m l r ->
    and
      [ -- Bin doesn't have Nil children
        not (isNil l),
        not (isNil r),
        -- mask is a power of two
        popCount m == 1,
        -- mask is the highest bit position at which two keys of the map differ, so the prefix to the left of the mask
        -- should be the same for every element, which should also be `p` (all 0s or all 1s)
        allSame p (map (`WordMap.mask` m) (wordMapKeysList l)) (map (`WordMap.mask` m) (wordMapKeysList r)),
        -- none of the left have the mask bit set
        all (\w -> w .&. m == 0) (wordMapKeysList l),
        -- all of the right have the mask bit set
        all (\w -> w .&. m /= 0) (wordMapKeysList r)
      ]
  WordMap.Tip _ _ -> True
  WordMap.Nil -> True
  where
    isNil :: WordMap a -> Bool
    isNil = \case
      WordMap.Nil -> True
      _ -> False

    allSame :: Word64 -> [Word64] -> [Word64] -> Bool
    allSame prefix xs ys =
      all (== prefix) xs && all (== prefix) ys

listToWordMap :: [(Word64, a)] -> WordMap a
listToWordMap =
  foldr (\(k, v) -> WordMap.insert k v) WordMap.empty

wordMapKeysList :: WordMap a -> [Word64]
wordMapKeysList =
  map fst . wordMapToList

wordMapToList :: WordMap a -> [(Word64, a)]
wordMapToList =
  List.unfoldr
    ( \m0 ->
        case WordMap.pop m0 of
          WordMap.PopNada -> Nothing
          WordMap.PopAlgo k v m1 -> Just ((k, v), m1)
    )
