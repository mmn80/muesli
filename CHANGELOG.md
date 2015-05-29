### 0.1.1.0

* fixed index update bug: new references were added, but the ones from the
previous version of the document were not removed
* added sort order argument to range queries
* `lookup` returns just `Maybe a`
* renamed `lookupUnique` to `unique'`
* renamed `filter` to `filterRange`
* renamed `rangeK` to `range'`
* changed argument order in `range`, `range'`, `filterRange` to match SQL
* added `unique`
* added `filter`
* added `filterRange'`, `filter'`

### 0.1.0.1

* fixed a GC bug: `swapDb` failed to swap

### 0.1

* initial release
