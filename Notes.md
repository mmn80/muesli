Invariants
----------

- All locks are held for at most **O(log n)** time.
This includes "big" procedures, like GC, that never "stop the world" for too long.
- All meta data (incl. indexes) reside in memory in a `MasterState` structure.
- All meta data is built incrementally, in **O(log n)** steps,
from the transaction log (no backtracking).
- No meta data resides in the data file, just serialized user "documents".
- No serialized user data is saved in the transaction log file, just keys, addresses and sizes.
- All primitive transactions in the user facing API are **O(log n)**.
- Index management is fully automatic, online (*O(log n)* with no rebuild steps), and hidden from user.

Memory Data Structures
----------------------

The following "Haskell" is just pseudocode.
Instead of functions and lists we will have `Data.IntMap`, `Data.IntSet`, etc.

A `newtype` based `Handle` wraps a `DBState` ADT that contains
`masterState :: MVar MasterState` and `dataState :: MVar DataState`.
The handle is created by an `open` IO action, used for running transaction monad actions, and closed by a `close` IO action.

### Master State

`mainIdx` is the primary index, holding records' locations in the data file.
`gaps` is used for fast allocation.
`sortIdx` and `refIdx` are inverted indexes.

`unqIdx` is used for unique fields.
The `IntVal` is computed with the `Hashable` instance of the value type.
The index is used for ensuring uniqueness, and also for lookup queries.

`sortIdx` indexes columns that hold Int-convertible values, like Date/Time.
It is used for range queries.

`refIdx` indexes document reference columns.
`[DID]` is sorted on the `PropertyKey` in the pair.
There can be multiple copies for the same main `PropertyKey`, but sorted on different columns.
Used for filter+range queries (filter of first prop, then range on second).

`logPend` and `logComp` contain pending and completed transactions.
The flag `keepTrans` is set during GC, to prevent Update Manager
to clean completed transactions from `logComp`.

```haskell
logHandle :: Handle
keepTrans :: Bool
logPend   :: TID -> [(DID, DocAddress, DocSize, Del, [(PropertyKey, [DID])])]
logComp   :: TID -> [(DID, DocAddress, DocSize, Del, [(PropertyKey, [DID])])]
gaps      :: DocSize -> [DocAddress]
mainIdx   :: DID -> [(TID, DocAddress, DocSize, [(PropertyKey, [DID])])]
unqIdx    :: PropertyKey -> IntVal -> DID
sortIdx    :: PropertyKey -> IntVal -> [DID]
refIdx    :: PropertyKey -> [(PropertyKey, DID -> [DID])]
```

### Data State

Cached deserialized records wrapped in `Data.Dynamic` are held in `dataCache`.
This is a big win over just memory mapping the data file,
since deserialization is by far the slowest part of reads.
Oldest records are removed at maximum capacity, or based on a maximum age.
In the latter case, records are removed only above a minimum capacity.
The cache uses the data file address as key.

```haskell
dataHandle :: Handle
dataCache  :: LRUCache
```

Data Files Format
-----------------

`DID`, `DocAddress`, `DocSize` and `PropertyKey` are aliases of `IxKey`, a newtype wrapper
around `Int`.
`TID` is auto-incremented `Word64`.
The tags for `Pending`, `Completed` and `Del` are single bytes.
`ValCount`, `UnqCount` and `RefCount` are `Word16`.
All of them are serialized with their `Storable` instance.

The pseudo-Haskell represents just a byte sequence in lexical order.

### Transaction Log File

```haskell
logPos :: DocAddress
recs   :: [TransRecord]

TransRecord = Pending TID DID DocAddress DocSize Del UnqCount [(PropertyKey, IntVal)]
                                     ValCount [(PropertyKey, IntVal)]
                                     RefCount [(PropertyKey, DID)]
     | Completed TID
```

### Data File

The data file is a sequence of `ByteString`s produced by the `Serialize` instance
of user data types, interspersed with gaps (old record versions cleaned by GC).

```haskell
recs :: [(ByteString, Gap)]
```

Initialization
--------------

All meta data resides in `masterState`, which is built incrementally from the transaction log.
Each record in the log leads to an *O(log(n))* `masterState` update operation.
No "wrapping up" needs to be performed at the end.
`masterState` remains consistent at every step.

Transaction Monad
-----------------

A State monad over IO that holds inside a `Handle`, a `TID`, a read list, and an update list.

Reads are executed live, while updates are just accumulated in the list.
The transaction is written in the log at the end, and contains the updated DID list.
Only this step is under lock.

While the record data is asynchronously written by the Update Manager,
other transactions can check the DID list of pending & completed transactions
and abort themselves in case of conflict.

Read queries target a specific version, `TID <= tid`, and are not blocked by writes.
Data access is under lock, but the cache will speed things up.

See: https://en.wikipedia.org/wiki/Multiversion_concurrency_control

### Primitive ops

```haskell
lookup :: DID a -> Trans (Maybe a)
insert :: a -> Trans (DID a)
update :: DID a -> a -> Trans ()
delete :: DID a -> Trans ()
```

Also range/page queries, and queries on the `sortIdx` and `unqIdx`.

### Running Transactions

```haskell
runQuery :: Handle -> Trans a -> IO (Maybe a)

ReadList   = [DID]
UpdateList = [(LogRecord, ByteString)]

```
```
begin:
  with master lock:
    tid <- generate TID
  init update list
  init read list
middle (the part users write):
  execute index lookups in-memory (with master lock and TID <= tid)
  execute document lookups in the DB (with data lock and TID <= tid)
  execute other user IO
  collect lookups to the read list
  collect updates to the update list
end:
  with master lock:
    check new transactions in masterState (both pending and completed):
      if they contain updated/deleted DIDs that clash with read list, abort
      if they contain deleted DIDs that clash with update list, abort
      if they contain updated DIDs that clash with update list, abort or ignore
        based on policy
    allocate space and update gaps accordingly
    add new transaction based on the update list to logPend
    update logPos, logSize
    logPos' := logPos + trans size
    write to transaction log:
      increase file size if < logPos'
      write records
      logPos := logPos'
```

Update Manager
--------------

We use an **ACId** with *'eventual durability'* model.
If the program dies before the Update Manager (asynchronously) finished
processing `logPend`, the `logPos` will never be updated (last operation, see below).
Next time the program starts the incomplete transaction will simply be ignored.
Since `gaps` are always rebuild from valid log data,
the previously allocated slots will become available again.

```
repeat:
  with UM lock:
    with master lock:
      get first (by TID) job from logPend
    with data lock:
      increase file size if needed
      write updates
    with master lock:
      remove transaction from logPend
      if keepTrans or new pending transactions exist, add it to logComp
      if logPend is empty and not keepTrans, empty logComp
      update mainIdx, unqIdx, sortIdx, refIdx
      add "Completed: TID" to the transaction log
      update logPos in the transaction log
```

Garbage Collector
-----------------

GC runs asynchronously, and can be executed at any time.

```
with master lock:
  grab masterState ref
  keepTrans = True
collect garbage from grabbed masterState
reallocate records contiguously (empty gaps)
rebuild mainIdx, unqIdx, sortIdx, refIdx
write new log file
write new data file
with UM lock:
  with master lock:
    update old logComp and logPend (reallocate)
    update new (empty) gaps from logComp and logPend
    write new transactions to new log & new data file
    update new mainIdx, unqIdx, sortIdx, refIdx from logComp
    swap log files (close old handle; rename file; open new handle)
    keepTrans = False
  with data lock:
    swap data files
```
