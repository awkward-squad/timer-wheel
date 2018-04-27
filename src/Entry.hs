{-# language LambdaCase #-}

module Entry where

newtype EntryId
  = EntryId Int
  deriving (Eq)

data Entry = Entry
  { entryId :: !EntryId
  , entryCount :: !Int
  , entryAction :: IO ()
  }

isExpired :: Entry -> Bool
isExpired =
  (== 0) . entryCount

decrement :: Entry -> Entry
decrement entry =
  entry { entryCount = entryCount entry - 1 }

delete :: EntryId -> [Entry] -> (Maybe Entry, [Entry])
delete i =
  go []
 where
  go :: [Entry] -> [Entry] -> (Maybe Entry, [Entry])
  go acc = \case
    [] ->
      (Nothing, acc)
    x:xs
      | i == entryId x ->
          (Just x, acc ++ xs)
      | otherwise ->
          go (x:acc) xs
