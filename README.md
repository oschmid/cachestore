cachestore
==========

Getting objects from datastore and loading them into memcache to speed up future reads is a common pattern.
Using the same method signatures as appengine.datastore, this package has all reads first check memcache before
calling datastore and all writes write to both memcache and datastore.

memcache.Items are created using datastore.Key.Encode() as the string key and gob encoded objects for values.