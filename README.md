cachestore
==========

This is a drop-in replacement of appengine/datastore that automatically caches structs and PropertyLoadSavers in memcache.
* If Get or GetMulti miss when reading from memcache, they fallback to reading from datastore and load the result into memcache for next time.
* Put and PutMulti write to memcache and datastore.
* Delete and DeleteMulti delete from memcache and datastore.

cachestore uses datastore keys and gob encoded values to create memcache items
