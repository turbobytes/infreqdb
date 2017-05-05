package infreqdb

import (
	"testing"
	"time"
)

func TestS3Storage(t *testing.T) {
	bucket, err := getmockbucket()
	if err != nil {
		t.Error(err)
	}
	var storage Storage
	storage = NewS3Storage(bucket, "/") //should be compile fail if interface is not implimented
	//Get 404
	fname, found, mutable, lastmod, err := storage.Get("foo")
	if err != nil {
		t.Error(err)
	}
	if fname != "" {
		t.Errorf("Expected blank fname, got %v", fname)
	}
	if found {
		t.Errorf("Expected to be not found, got found")
	}
	if !mutable {
		t.Errorf("Expected to be mutable, got not mutable")
	}
	if lastmod != time.Unix(2, 2) {
		t.Errorf("Expected to be %s, got not %s", time.Unix(2, 2), lastmod)
	}
}
