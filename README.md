muesli
======

An easy to use [document-oriented database](https://en.wikipedia.org/wiki/Document-oriented_database)
engine for Haskell.

Use cases
---------
* higher performance replacement for `acid-state`.
* "no external dependency" replacement for `SQLite` and the like.
* store for cloud/p2p nodes, mobile apps, etc.

Features
--------
* ACID transactions implemented on the [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) model.
* automatic index management based on tags added to your fields' types
(see example below).
* no `TemplateHaskell`, all boilerplate internally generated with
`GHC.Generics` and the new `deriving`.
* only `Prelude` functions for file I/O, no dependencies on C libraries
(mmap, kv stores, etc.)
* simple monad for writing queries, with standard primitive operations:
`lookup`, `insert`, `update`, `delete`, `range` and `filter`.
* most general type of query supported by the primitive ops (`filter`):
```SQL
SELECT TOP p * FROM t WHERE c = k AND o < s ORDER BY o DESC
```
* easy to reason about performance: all primitive queries run in **O(log n)**.
* type-safety: impossible to attempt deserializeng record at wrong type
(or address), and risk getting bogus data with no error thrown.
IDs are tagged with a phantom type and given to you by the polymorphic primitives.
There are also `Num`/`Integral` instances to support more generic database apps,
but normally you don't need to use those.

Example use
-----------
First, mark up your types. You must use the record syntax to name your
accessors so you can query later. You can filter on `DocID` and `Unique`
fields, and sort on `Indexable`. The database will extract these keys,
including from deep inside any `Foldable`.

```Haskell
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

import Database.Muesli.Types

data Person = Person
  { personName  :: Unique (Indexable String)
  , personEmail :: String
  } deriving (Show, Generic, Serialize)

instance Document Person

data Content  = Text String | HTML String | XHTML String
  deriving (Show, Generic, Serialize, DBValue)

data BlogPost = BlogPost
  { postURI          :: Unique String
  , postTitle        :: Indexable String
  , postAuthor       :: Maybe (DocID Person)
  , postContributors :: [DocID Person]
  , postTags         :: [Indexable String]
  , postContent      :: Content
  } deriving (Show, Generic, Serialize)

instance Document BlogPost
```

Then, write some queries:

```Haskell
{-# LANGUAGE OverloadedStrings #-}

import Database.Muesli.Query

-- searches for person by unique key, and inserts or updates it if exists
updatePerson :: String -> String -> Transaction (DocID Person, Person)
updatePerson name email = do
  let name' = Indexable name
  let p = Person name' email
  pid <- updateUnique "personName" (intValUnique name') p
  return (pid, p)

postsByContributor :: DocID Person -> Transaction [(DocID BlogPost, BlogPost)]
postsByContributor pid =
  filter (Just pid) Nothing Nothing "postContributors" "postTitle" 1000

flagContributor :: DocID Person -> Transaction ()
flagContributor pid = do
  is <- postsByContributor pid
  forM_ is $ \(bpid, bp) ->
    update bpid bp { postTags = postTags bp ++ Indexable "stolen" }
```

Then you can run these transactions with `runQuery` inside some `MonadIO` context.
Note that `Transaction` itself is an instance of `MonadIO`,
so you can do arbitrary IO inside.

```Haskell
import Database.Muesli.Query
import Database.Muesli.Handle

flagIt :: (MonadIO m) => Handle -> String -> String ->
                         m (Either TransactionAbort ())
flagIt h name email = runQuery h $ do
  (pid, _) <- updatePerson name email
  flagContributor pid

main :: IO ()
main = bracket
    (do putStrLn "opening DB..."
        open (Just "feeds.log") (Just "feeds.dat") )
    (\h -> do
        putStrLn "closing DB..."
        close h )
    (\h -> flagIt h "Bender Bending Rodríguez" "bender@ilovebender.com" )

```

TODO
----
* bug? lookups after updates in the same transaction
* expose the inverted index
* queries that only return keys (no data file IO)
* testing it on mobile devices
* static property names, but no ugly `Proxy :: Proxy "FieldName"` stuff
* support for extensible records ("lax" `Serialize` instance),
live up to the "document-oriented" label
* better migration story
* radix tree / PATRICIA implementation for proper full-text search
(currently indexing strings just takes first 4 chars and turnes them into int,
which is good enough for simple sorting)
* replication
* fancy query language
* waiting for [`OverloadedRecordFields`](https://ghc.haskell.org/trac/ghc/wiki/Records/OverloadedRecordFields)

Implementation
--------------
* 2 files, one for transactions/indexes, and another for serialized data
* same file format for transactions and indexes, loading indexes is
the same as replaying transactions
* transaction records only contain int keys
* data file only contains serialized records and gaps, no metadata
* LRU cache holds deserialized objects wrapped in `Data.Dynamic`
* GC creates asynchronously new copies of both files, doing cleanup and
compaction, and only locks the world at the end
* all locks are held for at most **O(log n)** time
* transactions defer updates by collecting IDs and serialized data,
which are checked (under lock) for consistency at the end

See the [notes](https://github.com/clnx/muesli/blob/master/Notes.md).
