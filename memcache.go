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

package cachestore

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"reflect"
)

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
