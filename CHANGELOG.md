### 0.1.1.0

* renamed `filter` to `rangeF`
* changed argument order in `range`, `rangeK`, `rangeF`
* added new query `filter` (just filters, without range)
* `lookup` returns just `Maybe a`
* renamed `lookupUnique` to `lookupUniqueK`
* added `lookupUnique` (that does something like `lookupUniqueK` >>= 'lookup')

### 0.1.0.1

* fixed a GC bug

### 0.1

* initial release
