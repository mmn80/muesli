### 0.1.1.0

* `lookup` returns just `Maybe a`
* renamed `lookupUnique` to `unique'`
* added `unique` (that does something like `unique'` >>= 'lookup')
* renamed `filter` to `filterRange`
* added `filter` (just filters, without range)
* renamed `rangeK` to `range'`
* added `filterRange'`, `filter'`
* changed argument order in `range`, `range'`, `filterRange` to resemble SQL

### 0.1.0.1

* fixed a GC bug

### 0.1

* initial release
