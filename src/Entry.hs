{-# language LambdaCase #-}

module Entry where

newtype EntryId
  = EntryId Int
  deriving (Eq)

instance Show EntryId where
  show (EntryId n) =
    show n

data Entry = Entry
  { entryId :: !EntryId
  , entryCount :: !Int
  , entryAction :: IO ()
  }

instance Show Entry where
  show entry =
    show (entryId entry) ++ "@" ++ show (entryCount entry)
