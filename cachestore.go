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
	"bytes"
	"encoding/gob"
	"reflect"
)

var Debug = false // If true, print debug info

// Get loads the entity stored for key (from memcached if it has been cached, datastore otherwise) into dst,
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
func GetMulti(c appengine.Context, key []*datastore.Key, dst interface{}) error {
	// check cache
	encodedKeys := encodeKeys(key)
	itemMap, errm := memcache.GetMulti(c, encodedKeys)
	if len(itemMap) != len(key) {
		// TODO benchmark loading all vs loading missing
		// load from datastore
		errd := datastore.GetMulti(c, key, dst)
		if Debug {
			c.Debugf("reading from store: %#v", dst)
		}
		if errd != nil {
			return errd
		}
		// cache for next time
		errm = cache(key, dst, c)
	} else {
		errm = decodeItems(key, itemMap, dst)
		if Debug {
			c.Debugf("reading from cache: %#v", dst)
		}
	}
	return errm
}

// encodeKeys returns an array of string encoded datastore.Keys
func encodeKeys(key []*datastore.Key) []string {
	encodedKeys := make([]string, len(key))
	for i, k := range key {
		encodedKeys[i] = k.Encode()
	}
	return encodedKeys
}

// cache writes structs and PropertyLoadSavers to memcache.
func cache(key []*datastore.Key, src interface{}, c appengine.Context) error {
	items, err := encodeItems(key, src)
	if len(items) > 0 && err == nil {
		if Debug {
			c.Debugf("writing to cache: %#v", src)
		}
		err = memcache.SetMulti(c, items)
	}
	return err
}

// encodeItems returns an array of memcache.Items for all key/value pair where the key is not incomplete.
func encodeItems(key []*datastore.Key, src interface{}) ([]*memcache.Item, error) {
	v := reflect.ValueOf(src)
	multiArgType, _ := checkMultiArg(v)
	items := *new([]*memcache.Item)
	for i, k := range key {
		if !k.Incomplete() {
			elem := v.Index(i)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				elem = elem.Addr()
			}
			value, err := encode(elem.Interface())
			if err != nil {
				return items, err
			}
			item := &memcache.Item{Key: k.Encode(), Value: value}
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

// decodeItems decodes items and writes them to dst.
func decodeItems(key []*datastore.Key, items map[string]*memcache.Item, dst interface{}) error {
	v := reflect.ValueOf(dst)
	multiArgType, _ := checkMultiArg(v)
	multiErr, any := make(appengine.MultiError, len(key)), false
	for i, k := range key {
		item := items[k.Encode()]
		if item == nil {
			multiErr[i] = datastore.ErrNoSuchEntity
		} else {
			d := v.Index(i)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				d = d.Addr()
			}
			multiErr[i] = decode(d.Interface(), item.Value)
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
func decode(dst interface{}, b []byte) (err error) {
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
		// gob encoded key pointers as keys, convert them back to pointers
		if key, ok := p.Value.(datastore.Key); ok {
			p.Value = &key
		}
		dst <- p
	}
	errc <- nil
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
	key, errd := datastore.PutMulti(c, key, src)
	if errd == nil {
		return key, cache(key, src, c)
	}
	return key, errd
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
