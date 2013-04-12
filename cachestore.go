/*
This file is part of cachestore.

cachestore is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

cachestore is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with cachestore.  If not, see <http://www.gnu.org/licenses/>.
*/

// Cachestore automatically caches structs in memcache using gob and the structs' encoded datastore.Key.
// Reads check memcache first, if they miss they read from datastore and write the results into memcache.
// Writes write to both memcache and datastore. Cachestore will try to write to the datastore even if an
// error occurs when writing to memcache.
//
// Types need to be registered with gob.Register(interface{}) for cachestore to be able to store them.
package cachestore

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"encoding/gob"
	"errors"
	"reflect"
	"time"
)

var Debug = false // If true, print debug info

// Batch size for calls to datastore.PutMulti with many Items. Default suggested by Andrew Gerrand (https://groups.google.com/d/msg/google-appengine-go/qNNH6VnFrY0/be9rrA1NZXMJ)
var BatchSize = 100

func init() {
	// register basic datastore types
	gob.Register(time.Time{})
	gob.Register(datastore.Key{})
}

// Get loads the entity stored for key (from memcached if it has been cached, datastore otherwise) into dst,
// which must be a struct pointer or implement PropertyLoadSaver. If there is no such entity for the key,
// Get returns ErrNoSuchEntity.
//
// The values of dst's unmatched struct fields are not modified. In particular, it is recommended to pass either
// a pointer or a zero valued struct on each Get call.
//
// ErrFieldMismatch is returned when a field is to be loaded into a different type than the one it was stored from,
// or when a field is missing or unexported in the destination struct. ErrFieldMismatch is only returned if dst is
// a struct pointer.
func Get(c appengine.Context, key *datastore.Key, dst interface{}) error {
	err := GetMulti(c, []*datastore.Key{key}, []interface{}{dst})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// GetMulti is a batch version of Get. Cached values are returned from memcache, uncached values are returned from
// datastore and memcached for next time.
//
// dst must be a []S, []*S, []I or []P, for some struct type S, some interface type I, or some non-interface
// non-pointer type P such that P or *P implements PropertyLoadSaver. If an []I, each element must be a valid
// dst for Get: it must be a struct pointer or implement PropertyLoadSaver.
//
// As a special case, PropertyList is an invalid type for dst, even though a PropertyList is a slice of structs.
// It is treated as invalid to avoid being mistakenly passed when []PropertyList was intended.
func GetMulti(c appengine.Context, key []*datastore.Key, dst interface{}) error {
	if len(key) == 0 {
		return nil
	}
	// check cache
	encodedKeys := encodeKeys(key)
	itemMap, errm := memcache.GetMulti(c, encodedKeys)
	if len(itemMap) != len(key) {
		// TODO benchmark loading all vs loading missing
		// load from datastore
		errd := datastore.GetMulti(c, key, dst)
		if Debug {
			c.Debugf("reading from datastore: %#v", dst)
		}
		if errd != nil {
			return errd
		}
		// cache for next time
		errm = cache(key, dst, c)
	} else {
		errm = decodeItems(key, itemMap, dst)
		if Debug {
			c.Debugf("reading from memcache: %#v", dst)
		}
	}
	return errm
}

// Put saves the entity src into datastore with key, and memcache if nothing goes wrong in saving to datastore.
// src must be a struct pointer or implement PropertyLoadSaver; if a struct pointer then any unexported fields
// of that struct will be skipped. If k is an incomplete key, the returned key will be a unique key generated
// by the datastore.
func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	k, err := PutMulti(c, []*datastore.Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return k[0], nil
}

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
func PutMulti(c appengine.Context, key []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	if Debug {
		c.Debugf("writing to datastore: %#v", src)
	}
	key, errd := batchPutMulti(c, key, src)
	if errd == nil {
		return key, cache(key, src, c)
	}
	return key, errd
}

// batchPutMulti splits calls to datastore.PutMulti into batches of size BatchSize to get around datastore.PutMulti's size limit.
func batchPutMulti(c appengine.Context, key []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	v := reflect.ValueOf(src)
	multiArgType, _ := checkMultiArg(v)
	if multiArgType == multiArgTypeInvalid {
		return nil, errors.New("datastore: src has invalid type")
	}
	if len(key) != v.Len() {
		return nil, errors.New("datastore: key and src slices have different length")
	}
	if len(key) == 0 {
		return nil, nil
	}
	if err := multiValid(key); err != nil {
		return nil, err
	}
	ret := make([]*datastore.Key, len(key))
	for lo := 0; lo <= v.Len(); lo += BatchSize {
		hi := lo + BatchSize
		if v.Len() < hi {
			hi = v.Len()
		}
		batchKey, err := datastore.PutMulti(c, key[lo:hi], v.Slice(lo, hi).Interface())
		if err != nil {
			return nil, err
		}
		copy(ret[lo:hi], batchKey)
	}
	return ret, nil
}

// Delete deletes the entity for the given key from memcache and datastore.
func Delete(c appengine.Context, key *datastore.Key) error {
	err := DeleteMulti(c, []*datastore.Key{key})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti is a batched version of Delete.
func DeleteMulti(c appengine.Context, key []*datastore.Key) error {
	errm := memcache.DeleteMulti(c, encodeKeys(key))
	errd := datastore.DeleteMulti(c, key)
	if errd != nil {
		return errd
	}
	return errm
}
