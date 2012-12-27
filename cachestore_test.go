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

package cachestore

import (
	"appengine/datastore"
	"appengine/memcache"
	"encoding/gob"
	"fmt"
	"github.com/oschmid/appenginetesting"
	"reflect"
	"testing"
)

var c = must(appenginetesting.NewContext(nil))

func must(c *appenginetesting.Context, err error) *appenginetesting.Context {
	if err != nil {
		panic(err)
	}
	return c
}

func init() {
	gob.Register(*new(Struct))
}

type Struct struct {
	I int
}

type PropertyLoadSaver struct {
	S string
}

func (p *PropertyLoadSaver) Load(c <-chan datastore.Property) error {
	if err := datastore.LoadStruct(p, c); err != nil {
		return err
	}
	p.S += ".load"
	return nil
}

func (p *PropertyLoadSaver) Save(c chan<- datastore.Property) error {
	defer close(c)
	c <- datastore.Property{
		Name:  "S",
		Value: p.S + ".save",
	}
	return nil
}

func TestDecodeStructArray(t *testing.T) {
	src, keys, items := encodeStructArray(t)
	dst := make([]Struct, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
}

func encodeStructArray(t *testing.T) ([]Struct, []*datastore.Key, []*memcache.Item) {
	src := *new([]Struct)
	keys := *new([]*datastore.Key)
	for i := 1; i <= 10; i++ {
		src = append(src, Struct{I: i})
		keys = append(keys, datastore.NewKey(c, "Struct", "", int64(i), nil))
	}
	items, err := encodeItems(keys, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(src) {
		t.Fatalf("expected=%d actual=%d", len(src), len(items))
	}
	return src, keys, items
}

func makeItemsMap(keys []*datastore.Key, items []*memcache.Item) map[string]*memcache.Item {
	itemsMap := make(map[string]*memcache.Item)
	for i, key := range keys {
		itemsMap[key.Encode()] = items[i]
	}
	return itemsMap
}

func TestDecodeStructPointerArray(t *testing.T) {
	src, keys, items := encodeStructPointerArray(t)
	dst := make([]*Struct, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
}

func encodeStructPointerArray(t *testing.T) ([]*Struct, []*datastore.Key, []*memcache.Item) {
	src := *new([]*Struct)
	keys := *new([]*datastore.Key)
	for i := 1; i <= 10; i++ {
		src = append(src, &Struct{I: i})
		keys = append(keys, datastore.NewKey(c, "Struct", "", int64(i), nil))
	}
	items, err := encodeItems(keys, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(src) {
		t.Fatalf("expected=%d actual=%d", len(src), len(items))
	}
	return src, keys, items
}

func TestDecodeStructArrayToStructPointerArray(t *testing.T) {
	src, keys, items := encodeStructArray(t)
	dst := make([]*Struct, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if !reflect.DeepEqual(*d, src[i]) {
			t.Fatalf("expected=%#v actual=%#v", src[i], *d)
		}
	}
}

func TestDecodeStructPointerArrayToStructArray(t *testing.T) {
	src, keys, items := encodeStructPointerArray(t)
	dst := make([]Struct, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if !reflect.DeepEqual(d, *src[i]) {
			t.Fatalf("expected=%#v actual=%#v", *src[i], d)
		}
	}
}

func TestDecodePropertyLoadSaverArray(t *testing.T) {
	src, keys, items := encodePropertyLoadSaverArray(t)
	dst := make([]PropertyLoadSaver, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if d.S != src[i].S+".save.load" {
			t.Fatal("actual=%v", d.S)
		}
	}
}

func encodePropertyLoadSaverArray(t *testing.T) ([]PropertyLoadSaver, []*datastore.Key, []*memcache.Item) {
	src := *new([]PropertyLoadSaver)
	keys := *new([]*datastore.Key)
	for i := 1; i <= 10; i++ {
		src = append(src, PropertyLoadSaver{S: fmt.Sprint(i)})
		keys = append(keys, datastore.NewKey(c, "PropertyLoadSaver", "", int64(i), nil))
	}
	items, err := encodeItems(keys, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(src) {
		t.Fatalf("expected=%d actual=%d", len(src), len(items))
	}
	return src, keys, items
}

func TestDecodePropertyLoadSaverPointerArray(t *testing.T) {
	src, keys, items := encodePropertyLoadSaverPointerArray(t)
	dst := make([]*PropertyLoadSaver, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if d.S != src[i].S+".save.load" {
			t.Fatalf("actual=%v", d.S)
		}
	}
}

func encodePropertyLoadSaverPointerArray(t *testing.T) ([]*PropertyLoadSaver, []*datastore.Key, []*memcache.Item) {
	src := *new([]*PropertyLoadSaver)
	keys := *new([]*datastore.Key)
	for i := 1; i <= 10; i++ {
		src = append(src, &PropertyLoadSaver{S: fmt.Sprint(i)})
		keys = append(keys, datastore.NewKey(c, "PropertyLoadSaver", "", int64(i), nil))
	}
	items, err := encodeItems(keys, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(src) {
		t.Fatalf("expected=%d actual=%d", len(src), len(items))
	}
	return src, keys, items
}

func TestDecodePropertyLoadSaverArrayToPointerArray(t *testing.T) {
	src, keys, items := encodePropertyLoadSaverArray(t)
	dst := make([]*PropertyLoadSaver, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if d.S != src[i].S+".save.load" {
			t.Fatalf("actual=%v", d.S)
		}
	}
}

func TestDecodePropertyLoadSaverPointerArrayToArray(t *testing.T) {
	src, keys, items := encodePropertyLoadSaverPointerArray(t)
	dst := make([]PropertyLoadSaver, len(src))
	itemsMap := makeItemsMap(keys, items)
	if err := decodeItems(keys, itemsMap, dst); err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if d.S != src[i].S+".save.load" {
			t.Fatalf("actual=%v", d.S)
		}
	}
}
