/*
This file is part of Tessernote.

Tessernote is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Tessernote is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Tessernote.  If not, see <http://www.gnu.org/licenses/>.
*/

// Handles memcache-ing objects transparently when making datastore calls.
// Uses datastore.Key.Encode() as memcache.Item.Key and gob encoded values.
// memcache errors are written to appengine.Context.Warningf and otherwise ignored.
package cachestore

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"reflect"
)

// Delete deletes the entity for the given key from memcache and datastore.
func Delete(c appengine.Context, key *datastore.Key) error {
	err := memcache.Delete(c, key.Encode())
	if err != nil {
		c.Warningf(err.Error())
	}
	return datastore.Delete(c, key)
}

// DeleteMulti is a batched version of Delete
func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	err := memcache.DeleteMulti(c, encodeKeys(keys))
	if err != nil {
		c.Warningf(err.Error())
	}
	return datastore.DeleteMulti(c, keys)
}

// Get loads the entity stored for k (from memcached if it has been cached, datastore otherwise) into dst,
// which must be a struct pointer or implement PropertyLoadSaver. If there is no such entity for the key,
// Get returns ErrNoSuchEntity.
//
// The values of dst's unmatched struct fields are not modified. In particular, it is recommended to pass either
// a pointer to a zero valued struct on each Get call.
//
// ErrFieldMismatch is returned when a field is to be loaded into a different type than the one it was stored from,
// or when a field is missing or unexported in the destination struct. ErrFieldMismatch is only returned if dst is
// a struct pointer.
func Get(c appengine.Context, key *datastore.Key, dst interface{}) error {
	// check cache
	item, err := memcache.Get(c, key.Encode())
	if err != nil {
		if err != memcache.ErrCacheMiss {
			c.Warningf(err.Error())
		}
		// load from datastore
		err = datastore.Get(c, key, dst)
		if err != nil {
			return err
		}
		// cache for next time
		value, err := encode(dst)
		if err == nil {
			item = &memcache.Item{Key: key.Encode(), Value: value}
			err = memcache.Set(c, item)
		}
		if err != nil {
			c.Warningf(err.Error())
		}
		return nil
	}
	return decode(item.Value, dst)
}

// encode turns e into gob encoded bytes
func encode(e interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(e)
	return buffer.Bytes(), err
}

// decode decodes gob encoded bytes and writes them to e
func decode(value []byte, e interface{}) error {
	reader := bytes.NewReader(value)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(e)
}

// GetOnly is like Get but it doesn't write to memcache on a cache miss instead it just reads from datastore.
func GetOnly(c appengine.Context, key *datastore.Key, dst interface{}) error {
	item, err := memcache.Get(c, key.Encode())
	if err != nil {
		return datastore.Get(c, key, dst)
	}
	return decode(item.Value, dst)
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
func GetMulti(c appengine.Context, keys []*datastore.Key, dst interface{}) error {
	// check cache
	encodedKeys := encodeKeys(keys)
	itemMap, err := memcache.GetMulti(c, encodedKeys)
	if err != nil {
		c.Warningf(err.Error())
	}
	if len(itemMap) != len(keys) {
		// TODO benchmark loading all vs loading missing
		// load from datastore
		err = datastore.GetMulti(c, keys, dst)
		if err != nil {
			return err
		}
		// cache for next time
		items, err := encodeItems(keys, dst)
		if err == nil {
			err = memcache.SetMulti(c, items)
		}
		if err != nil {
			c.Warningf(err.Error())
		}
		return nil
	}
	return decodeItems(keys, itemMap, dst)
}

// encodeKeys returns an array of string encoded datastore.Keys
func encodeKeys(keys []*datastore.Key) []string {
	encodedKeys := make([]string, len(keys))
	for i, key := range keys {
		encodedKeys[i] = key.Encode()
	}
	return encodedKeys
}

// encodeItems returns an array of memcache.Items for all key/value pair where the key is not incomplete.
func encodeItems(keys []*datastore.Key, values interface{}) ([]*memcache.Item, error) {
	v := reflect.ValueOf(values)
	items := *new([]*memcache.Item)
	for i, key := range keys {
		if !key.Incomplete() {
			value, err := encode(v.Index(i))
			if err != nil {
				return items, err
			}
			item := &memcache.Item{Key: key.Encode(), Value: value}
			items = append(items, item)
		}
	}
	return items, nil
}

// decodeItems decodes items and writes them to dst. Because items are stored in a map,
// keys are needed to maintain order.
func decodeItems(keys []*datastore.Key, items map[string]*memcache.Item, dst interface{}) error {
	v := reflect.ValueOf(dst)
	multiArgType, _ := checkMultiArg(v)
	for i, key := range keys {
		item := items[key.Encode()]
		elem := v.Index(i)
		if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
			elem = elem.Addr()
		}
		// TODO fix
		err := decode(item.Value, elem.Interface())
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO GetOnlyMulti

// Put saves the entity src into memcache using memcache.Set() and datastore with key k. src must be a struct
// pointer or implement PropertyLoadSaver; if a struct pointer then any unexported fields of that struct will
// be skipped. If k is an incomplete key, the returned key will be a unique key generated by the datastore.
func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	if !key.Incomplete() {
		// cache
		value, err := encode(src)
		if err == nil {
			item := memcache.Item{Key: key.Encode(), Value: value}
			err = memcache.Set(c, &item)
			if err != nil {
				c.Warningf(err.Error())
			}
		} else {
			c.Warningf(err.Error())
		}
	}
	// store
	return datastore.Put(c, key, src)
}

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
func PutMulti(c appengine.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	// cache
	items, err := encodeItems(keys, src)
	if err == nil {
		err = memcache.SetMulti(c, items)
	}
	if err != nil {
		c.Warningf(err.Error())
	}
	// store
	return datastore.PutMulti(c, keys, src)
}
