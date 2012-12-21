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
package cachestore

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"strings"
	"reflect"
	"errors"
)

// Delete deletes the entity for the given key from memcache and datastore.
func Delete(c appengine.Context, key *datastore.Key) error {
	cacheErr := memcache.Delete(c, key.Encode())
	if cacheErr == memcache.ErrCacheMiss {
		cacheErr = nil
	}
	storeErr := datastore.Delete(c, key)
	if storeErr != nil {
		return storeErr
	}
	return cacheErr
}

// DeleteMulti is a batched version of Delete
func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	cacheErr := memcache.DeleteMulti(c, encodeKeys(keys))
	storeErr := datastore.DeleteMulti(c, keys)
	if storeErr != nil {
		return storeErr
	}
	return cacheErr
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
	if err == memcache.ErrCacheMiss {
		// load
		err = datastore.Get(c, key, dst)
		if err != nil {
			return err
		}
		// encode
		value, err := encode(dst)
		if err != nil {
			return err
		}
		// cache
		item = &memcache.Item{Key: key.Encode(), Value: value}
		err = memcache.Add(c, item)
		return err
	} else if err != nil {
		return err
	}
	// decode
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
	reader := strings.NewReader(string(value))
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(e)
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
//	items, err := memcache.GetMulti(c, encodeKeys(keys))
//	if err == memcache.ErrCacheMiss {
//		// TODO load from store
//		// TODO encode
//		// TODO cache
//	}
	// TODO check for missing items
	// TODO load missing items from store
	// TODO encode missing items
	// TODO cache missing items
	// TODO decode
	return datastore.GetMulti(c, keys, dst)
}

// encodeKeys returns an array of string encoded datastore.Keys
func encodeKeys(keys []*datastore.Key) []string {
	encodedKeys := *new([]string)
	for i, key := range keys {
		encodedKeys[i] = key.Encode()
	}
	return encodedKeys
}

// LoadStruct loads the properties from c to dst, reading from c until closed. dst must be a struct pointer.
//func LoadStruct(dst interface{}, c <-chan Property) error {
//	// TODO implement
//	panic("not yet implemented")
//	return nil
//}

// Put saves the entity src into memcache and datastore with key k. src must be a struct pointer or implement
// PropertyLoadSaver; if a struct pointer then any unexported fields of that struct will be skipped. If k is
// an incomplete key, the returned key will be a unique key generated by the datastore.
func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	// TODO check for incomplete keys
	// encode
	value, err := encode(src)
	if err != nil {
		return key, err
	}
	// cache
	item := memcache.Item{Key: key.Encode(), Value: value}
	err = memcache.Set(c, &item)
	if err != nil {
		return key, err
	}
	// store
	return datastore.Put(c, key, src)
}

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
func PutMulti(c appengine.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	// TODO check for incomplete keys
	// encode
	items, err := encodeItems(keys, src)
	// cache
	err = memcache.SetMulti(c, items)
	if err != nil {
		return keys, err
	}
	// store
	return datastore.PutMulti(c, keys, src)
}

// encodeItems returns an array of memcache.Items containing the
func encodeItems(keys []*datastore.Key, values interface{}) ([]*memcache.Item, error) {
	v := reflect.ValueOf(values)
	multiArgType, _ := checkMultiArg(v)
	if multiArgType == multiArgTypeInvalid {
		return nil, errors.New("datastore: src has invalid type")
	}
	if len(keys) != v.Len() {
		return nil, errors.New("datastore: key and src slices have different length")
	}
	if len(keys) == 0 {
		return nil, nil
	}

	items := *new([]*memcache.Item)
	for i, key := range keys {
		value, err := encode(v.Index(i))
		if err != nil {
			return items, err
		}
		items[i] = &memcache.Item{Key:key.Encode(),Value:value}
	}
	return items, nil
}

// SaveStruct saves the properties from src to c, closing c when done. src must be a struct pointer.
//func SaveStruct(src interface{}, c chan<- Property) error {
//	panic("not yet implemented")
//	return nil
//}
