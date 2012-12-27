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

// Automatically caches objects in memcache using gob and the objects encoded datastore.Key.
// Reads check memcache and fallback to datastore. Writes write to both memcache and datastore.
// Datastore consistency is guarranteed. Even if an error occurs in memcache, objects are still
// written to datastore. Memcache errors are logged to appengine.Context.
//
// Types need to be registered with gob.Register(interface{}) for cachestore to be able to store them.
package cachestore

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"reflect"
)

var Debug = false // If true, print debug info

// Delete deletes the entity for the given key from memcache and datastore.
func Delete(c appengine.Context, key *datastore.Key) error {
	err := DeleteMulti(c, []*datastore.Key{key})
	if me, ok := err.(appengine.MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti is a batched version of Delete
func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	if err := memcache.DeleteMulti(c, encodeKeys(keys)); err != nil {
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
		if Debug {
			c.Debugf("reading from store: %#v", dst)
		}
		if err != nil {
			return err
		}
		// cache for next time
		cache(keys, dst, c)
	} else {
		err = decodeItems(keys, itemMap, dst)
		if Debug {
			c.Debugf("reading from cache: %#v", dst)
		}
	}
	return err
}

func cache(keys []*datastore.Key, src interface{}, c appengine.Context) {
	items, err := encodeItems(keys, src)
	if len(items) > 0 && err == nil {
		if Debug {
			c.Debugf("writing to cache: %#v", src)
		}
		err = memcache.SetMulti(c, items)
	}
	if err != nil {
		c.Warningf(err.Error())
	}
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
	multiArgType, _ := checkMultiArg(v)
	items := *new([]*memcache.Item)
	for i, key := range keys {
		if !key.Incomplete() {
			elem := v.Index(i)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				elem = elem.Addr()
			}
			value, err := encode(elem.Interface())
			if err != nil {
				return items, err
			}
			item := &memcache.Item{Key: key.Encode(), Value: value}
			items = append(items, item)
		}
	}
	return items, nil
}

// encode encodes src using gob.Encoder
func encode(src interface{}) (b []byte, err error) {
	c := make(chan datastore.Property, 32)
	donec := make(chan struct{})
	go func() {
		b, err = propertiesToGob(c)
		close(donec)
	}()
	var err1 error
	if e, ok := src.(datastore.PropertyLoadSaver); ok {
		err1 = e.Save(c)
	} else {
		err1 = datastore.SaveStruct(src, c)
	}
	<-donec
	if err1 != nil {
		return nil, err1
	}
	return b, err
}

func propertiesToGob(src <-chan datastore.Property) ([]byte, error) {
	defer func() {
		for _ = range src {
			// Drain the src channel, if we exit early.
		}
	}()
	properties := *new([]datastore.Property)
	for p := range src {
		properties = append(properties, p)
	}
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(properties)
	return buffer.Bytes(), err
}

// decodeItems decodes items and writes them to dst. Because items are stored in a map,
// keys are needed to maintain order.
func decodeItems(keys []*datastore.Key, items map[string]*memcache.Item, dst interface{}) error {
	v := reflect.ValueOf(dst)
	multiArgType, _ := checkMultiArg(v)
	multiErr, any := make(appengine.MultiError, len(keys)), false
	for i, key := range keys {
		item := items[key.Encode()]
		if item == nil {
			multiErr[i] = datastore.ErrNoSuchEntity
		} else {
			d := v.Index(i)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				d = d.Addr()
			}
			multiErr[i] = decode(item.Value, d.Interface())
		}
		if multiErr[i] != nil {
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

// decode decodes b into dst using a gob.Decoder
func decode(b []byte, dst interface{}) (err error) {
	c := make(chan datastore.Property, 32)
	errc := make(chan error, 1)
	defer func() {
		if err == nil {
			err = <-errc
		}
	}()
	go gobToProperties(c, errc, b)
	if e, ok := dst.(datastore.PropertyLoadSaver); ok {
		return e.Load(c)
	}
	return datastore.LoadStruct(dst, c)
}

func gobToProperties(dst chan<- datastore.Property, errc chan<- error, b []byte) {
	defer close(dst)
	var properties []datastore.Property
	reader := bytes.NewReader(b)
	decoder := gob.NewDecoder(reader)
	if err := decoder.Decode(&properties); err != nil {
		errc <- err
		return
	}
	for _, p := range properties {
		dst <- p
	}
	errc <- nil
}

// Put saves the entity src into memcache using memcache.Set() and datastore with key k. src must be a struct
// pointer or implement PropertyLoadSaver; if a struct pointer then any unexported fields of that struct will
// be skipped. If k is an incomplete key, the returned key will be a unique key generated by the datastore.
func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	keys, err := PutMulti(c, []*datastore.Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return keys[0], nil
}

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
func PutMulti(c appengine.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	keys, err := datastore.PutMulti(c, keys, src)
	cache(keys, src, c)
	return keys, err
}
