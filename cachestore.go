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

// TODO separate out from tessernote, this could be a pretty valuable GAE library

/*
Handles caching objects in memcache transparently and provides
a convenient place to intercept datastore calls for testing purposes.
*/
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

func encode(e interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(e)
	return buffer.Bytes(), err
}

func decode(value []byte, e interface{}) error {
	reader := strings.NewReader(string(value))
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(e)
}

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

func encodeKeys(keys []*datastore.Key) []string {
	encodedKeys := *new([]string)
	for i, key := range keys {
		encodedKeys[i] = key.Encode()
	}
	return encodedKeys
}

func encodeItems(keys []string, values interface{}) ([]*memcache.Item, error) {
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
		items[i] = &memcache.Item{Key:key,Value:value}
	}
	return items, nil
}

func Put(c appengine.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
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

func PutMulti(c appengine.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	// encode
	items, err := encodeItems(encodeKeys(keys), src)
	// cache
	err = memcache.SetMulti(c, items)
	if err != nil {
		return keys, err
	}
	// store
	return datastore.PutMulti(c, keys, src)
}

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

func DeleteMulti(c appengine.Context, keys []*datastore.Key) error {
	cacheErr := memcache.DeleteMulti(c, encodeKeys(keys))
	storeErr := datastore.DeleteMulti(c, keys)
	if storeErr != nil {
		return storeErr
	}
	return cacheErr
}
