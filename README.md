muesli
======

A simple [document-oriented database][nosql] engine for Haskell.

Use cases
---------
* store for p2p / cloud nodes, mobile apps, etc.
* higher capacity replacement for *acid-state* (only indexes are held in memory).
* "no external dependency" substitute for *SQLite*, *CouchDB* and the like
(potentially easier to build and use on mobile devices, or even port to *GHCJS*).

Features
--------
* **ACID transactions** implemented on the [MVCC][] model.
* **automatic index management** based on tags added to fields' types
(see example below).
* **minimal boilerplate**: instead of *TemplateHaskell* we use
[`GHC.Generics`][gen] and [`deriving`][der].
* **simple monad for writing queries**, with standard primitive operations like:
`lookup`, `insert`, `update`, `delete`, `range`, `filter`.
* most general type of read query supported by the primitive ops (`filter`):
```SQL
SELECT TOP p * FROM t WHERE c = k AND o < s ORDER BY o DESC
```
* **easy to reason about performance**: all primitive queries run in **O(log n)**.
* **type safety**: impossible to attempt deserializing a record at a wrong type
(or address), and risk getting bogus data with no error thrown.
IDs are tagged with a phantom type and created only by the database.
There are also `Num`/`Integral` instances to support more generic apps,
but normally those are not needed.
* only `Prelude` functions for file I/O, **no dependencies on C libraries**
(mmap, kv stores, etc.)

*Note: some of these features become misfeatures for certain scenarios when
either a pure in-memory cache, or a "real" database may be more appropriate.*

Example use
-----------
First, mark up your types. You must use the record syntax to name the
accessors so they'll be queryable. You can filter on `Reference` fields, sort and
range on `Sortable`s, and reverse lookup `Unique`s. The database will extract
these keys using the `Indexable` and `Document` instances with the help of
`GHC.Generics`, including from deep inside any `Foldable`.

```Haskell
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

import Database.Muesli.Types

data Person = Person
  { personName  :: Unique (Sortable String)
  , personEmail :: String
  } deriving (Show, Generic, Serialize)

instance Document Person

data Content  = Text String | HTML String | XHTML String
  deriving (Show, Generic, Serialize, Indexable)

data BlogPost = BlogPost
  { postURI          :: Unique String
  , postTitle        :: Sortable String
  , postAuthor       :: Maybe (Reference Person)
  , postContributors :: [Reference Person]
  , postTags         :: [Sortable String]
  , postContent      :: Content
  } deriving (Show, Generic, Serialize)

instance Document BlogPost
```

Then, write some queries (`updateUnique` searches by unique key, and either
inserts or updates depending on result):

```Haskell
{-# LANGUAGE OverloadedStrings #-}

import Database.Muesli.Query

updatePerson :: String -> String -> Transaction (Reference Person, Person)
updatePerson name email = do
  let name' = Sortable name
  let p = Person name' email
  pid <- updateUnique "personName" (Unique name') p
  return (pid, p)

postsByContributor :: Reference Person -> Transaction [(Reference BlogPost, BlogPost)]
postsByContributor pid =
  filter (Just pid) Nothing Nothing "postContributors" "postTitle" 1000

flagContributor :: Reference Person -> Transaction ()
flagContributor pid = do
  is <- postsByContributor pid
  forM_ is $ \(bpid, bp) ->
    update bpid bp { postTags = postTags bp ++ Sortable "stolen" }
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
  (putStrLn "opening DB..." >> open (Just "blog.log") (Just "blog.dat"))
  (\h -> putStrLn "closing DB..." >> close h)
  (\h -> flagIt h "Bender Bending Rodríguez" "bender@ilovebender.com")

```

TODO
----
- [ ] expose the inverted index
- [ ] queries that only return keys (no data file IO)
- [ ] blocking version of `runQuery`
- [ ] testing it on mobile devices
- [ ] static property names, but no ugly `Proxy :: Proxy "FieldName"` stuff
- [ ] support for extensible records ("lax" `Serialize` instance),
live up to the "document-oriented" label, but this should be optional
- [ ] better migration story
- [ ] radix tree / PATRICIA implementation for proper full-text search
(currently indexing strings just takes first 4 chars and turnes them into an int,
which is good enough for simple sorting)
- [ ] replication
- [ ] more advanced & flexible index system supporting complex indexes, joins, etc.
- [ ] fancy query language
- [x] waiting for [`OverloadedRecordFields`][orf]

Implementation
--------------
* 2 files, one for transactions/indexes, and another for serialized data
* same file format for transactions and indexes, loading indexes is
the same as replaying transactions
* transaction file only contains int keys extracted from tagged fields
* processing a record (updating indexes) while loading the log is **O(log n)**
* previous 2 points make the initial loading much faster and using significantly
less memory then *acid-state*, which serializes entire records, including
potentially very large string fields, typical in "document-oriented" scenarios.
It was suggested that in such cases you should store this data in external files.
But then, if you want to regain the ACID property, and already have some indexes
laying aroung, you are well on your way of creating *muesli*.
* data file only contains serialized records and gaps, no metadata
* LRU cache holds deserialized objects wrapped in `Data.Dynamic`.
On SSDs deserialization is far more costly than file IO,
so having our own cache is a better solution than just memory mapping the file.
* :recycle: GC creates asynchronously new copies of both files, doing cleanup and
compaction, and only locks the world at the end
* :lock: all locks are held for at most **O(log n)** time
* `Reference`, `Unique` and `Sortable` are `newtype`s that have a set of general
instances for `Indexable` and `Document` which are used by a [generic function][gen]
* transactions defer updates by collecting IDs and serialized data,
which are checked (under lock) for consistency at the end

See the early [notes][].

Change log
----------
Available [here][changes].

License
-------
Copyright © 2015 Călin Ardelean

MIT license. See the [license file][MIT] for details.

[nosql]: https://en.wikipedia.org/wiki/Document-oriented_database "Document-oriented database - Wikipedia"
[MVCC]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control "Multiversion concurrency control - Wikipedia"
[gen]: https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/generic-programming.html "Generic Programming - GHC User's Guide"
[der]: https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/deriving.html "Extensions to the deriving mechanism - GHC User's Guide"
[orf]: https://ghc.haskell.org/trac/ghc/wiki/Records/OverloadedRecordFields "Overloaded Record Fields - GHC Wiki"
[notes]: https://github.com/clnx/muesli/blob/master/Notes.md "Muesli Implementation Notes"
[changes]: https://github.com/clnx/muesli/blob/master/CHANGELOG.md "Muesli change log"
[MIT]: https://github.com/clnx/muesli/blob/master/LICENSE.md "MIT License File"