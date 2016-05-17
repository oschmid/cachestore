package cachestore

import (
	"encoding/gob"
	"fmt"
	"reflect"
	"testing"

	"appengine"
	"appengine/aetest"
	"appengine/datastore"
)

var c = must(aetest.NewContext(nil))

func must(c aetest.Context, err error) aetest.Context {
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

func TestWithStruct(t *testing.T) {
	src := Struct{I: 3}
	key := datastore.NewIncompleteKey(c, "Struct", nil)
	// Put
	key, err := Put(c, key, &src)
	if err != nil {
		t.Fatal(err)
	}
	// Get
	dst := *new(Struct)
	err = Get(c, key, &dst)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
	// Delete
	err = Delete(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = Get(c, key, &dst)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("expected=%#v actual=%#v", datastore.ErrNoSuchEntity, err)
	}
}

func TestWithStructArray(t *testing.T) {
	src := *new([]Struct)
	key := *new([]*datastore.Key)
	for i := 1; i < 11; i++ {
		src = append(src, Struct{I: i})
		key = append(key, datastore.NewIncompleteKey(c, "Struct", nil))
	}
	// PutMulti
	key, err := PutMulti(c, key, src)
	if err != nil {
		t.Fatal(err)
	}
	// GetMulti
	dst := make([]Struct, len(src))
	err = GetMulti(c, key, dst)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
	// DeleteMulti
	err = DeleteMulti(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = GetMulti(c, key, dst)
	if me, ok := err.(appengine.MultiError); ok {
		for _, e := range me {
			if e != datastore.ErrNoSuchEntity {
				t.Fatal(e)
			}
		}
	} else {
		t.Fatal(err)
	}
}

// TODO test []*S

// TODO test []I

func TestWithPropertyLoadSaver(t *testing.T) {
	src := PropertyLoadSaver{}
	key := datastore.NewIncompleteKey(c, "PropertyLoadSaver", nil)
	// Put
	key, err := Put(c, key, &src)
	if err != nil {
		t.Fatal(err)
	}
	// Get
	dst := *new(PropertyLoadSaver)
	err = Get(c, key, &dst)
	if err != nil {
		t.Fatal(err)
	}
	if dst.S != src.S+".save.load" {
		t.Fatalf("actual=%#v", dst.S)
	}
	// Delete
	err = Delete(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = Get(c, key, &dst)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("expected=%#v actual=%#v", datastore.ErrNoSuchEntity, err)
	}
}

func TestWithPropertyLoadSaverArray(t *testing.T) {
	src := *new([]PropertyLoadSaver)
	key := *new([]*datastore.Key)
	for i := 1; i < 11; i++ {
		src = append(src, PropertyLoadSaver{S: fmt.Sprint(i)})
		key = append(key, datastore.NewIncompleteKey(c, "PropertyLoadSaver", nil))
	}
	// PutMulti
	key, err := PutMulti(c, key, src)
	if err != nil {
		t.Fatal(err)
	}
	// GetMulti
	dst := make([]PropertyLoadSaver, len(src))
	err = GetMulti(c, key, dst)
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range dst {
		if d.S != src[i].S+".save.load" {
			t.Fatalf("actual%#v", d.S)
		}
	}
	// DeleteMulti
	err = DeleteMulti(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = GetMulti(c, key, dst)
	if me, ok := err.(appengine.MultiError); ok {
		for _, e := range me {
			if e != datastore.ErrNoSuchEntity {
				t.Fatal(e)
			}
		}
	} else {
		t.Fatal(err)
	}
}

func TestGetFromMemcache(t *testing.T) {
	src := Struct{I: 3}
	key := datastore.NewIncompleteKey(c, "Struct", nil)
	// Put
	key, err := Put(c, key, &src)
	if err != nil {
		t.Fatal(err)
	}
	// load memcache with Get
	dst := *new(Struct)
	err = Get(c, key, &dst)
	if err != nil {
		t.Fatal(err)
	}
	// remove from datastore
	err = datastore.Delete(c, key)
	if err != nil {
		t.Fatal(err)
	}
	// Get
	err = Get(c, key, &dst)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
	// Delete
	err = Delete(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = Get(c, key, &dst)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("expected=%#v actual=%#v", datastore.ErrNoSuchEntity, err)
	}
}

func TestGetFromDatastore(t *testing.T) {
	src := Struct{I: 3}
	key := datastore.NewIncompleteKey(c, "Struct", nil)
	// Put
	key, err := Put(c, key, &src)
	if err != nil {
		t.Fatal(err)
	}
	// Get
	dst := *new(Struct)
	err = Get(c, key, &dst)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("expected=%#v actual=%#v", src, dst)
	}
	// Delete
	err = Delete(c, key)
	if err != nil {
		t.Fatal(err)
	}
	err = Get(c, key, &dst)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("expected=%#v actual=%#v", datastore.ErrNoSuchEntity, err)
	}
}
