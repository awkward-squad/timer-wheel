module Main (main) where

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
  [ ( "insert-lookup",
      Hedgehog.property do
        keys <- Hedgehog.forAll (Gen.list (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        let m = listToWordMap (map (,()) keys)
        for_ keys \key ->
          WordMap.lookup key m === Just ()
    ),
    ( "insert-pop",
      Hedgehog.property do
        keys <- Set.toList <$> Hedgehog.forAll (Gen.set (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        keys === wordMapKeysList (listToWordMap (map (,()) keys))
    ),
    ( "insert-splitL",
      Hedgehog.property do
        keys <- Hedgehog.forAll (Gen.list (Range.linear 1 1000) (Gen.word64 Range.linearBounded))
        key <- Hedgehog.forAll (Gen.word64 Range.linearBounded)
        let WordMap.Pair xs ys = WordMap.splitL key (listToWordMap (map (,()) keys))
        Hedgehog.assert (all (<= key) (wordMapKeysList xs))
        Hedgehog.assert (all (> key) (wordMapKeysList ys))
    )
  ]

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
