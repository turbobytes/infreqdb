package infreqdb

import (
	"testing"
	"time"
)

func TestDB(t *testing.T) {
	bucket, err := getmockbucket()
	if err != nil {
		t.Error(err)
	}
	db, err := New(bucket, "/foo/", 200)
	if err != nil {
		t.Error(err)
	}
	//Test invalid partid
	_, err = db.Get("what/ever", "foo")
	if err == nil {
		t.Error("Should have error, got none")
	}
	//Test err key does not exist
	_, err = db.Get("whatever", "foo")
	if err == nil {
		t.Error("Should have error, got none")
	}
	mockdata := map[string]interface{}{
		"foo": "bar",
	}
	err = db.SetPart("whatever", mockdata)
	if err != nil {
		t.Error(err)
	}
	//Try to get same partition
	item, err := db.Get("whatever", "foo")
	if err != nil {
		t.Error(err)
	}
	if item != "bar" {
		t.Errorf("expected bar, got %v", item)
	}
	//Run CheckExpiry() loop and make sure nothing is expired
	count := db.CheckExpiry()
	if count != 0 {
		t.Errorf("expected 0 expires, got %v", count)
	}
	//YIKES: Sleep a second since http time is at 1 second resolution
	time.Sleep(time.Second)
	//manually sideload new data, asif some other process or server changed the object
	mockdata["baz"] = "something"
	err = upLoadPartition("/foo/whatever", mockdata, bucket)
	if err != nil {
		t.Error(err)
	}
	//Confirm we cant read new value
	_, err = db.Get("whatever", "baz")
	if err == nil {
		t.Error("Should have error, got none")
	}
	//Run CheckExpiry() loop and make sure 1 item got expired
	count = db.CheckExpiry()
	if count != 1 {
		t.Errorf("expected 1 expires, got %v", count)
	}
	//Make sure we can read new value
	item, err = db.Get("whatever", "baz")
	if err != nil {
		t.Error(err)
	}
	if item != "something" {
		t.Errorf("expected something, got %v", item)
	}
}
