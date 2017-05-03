package infreqdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func TestCache(t *testing.T) {
	path := "/foo/bar"
	bucket, err := getmockbucket()
	if err != nil {
		t.Error(err)
	}
	//Populate a local file...
	tf := gettmpfile(t)
	defer os.Remove(tf)
	db, err := bolt.Open(tf, 0600, nil)
	if err != nil {
		t.Error(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucket([]byte("MyBucket"))
		if e != nil {
			return fmt.Errorf("create bucket: %s", e)
		}
		return b.Put([]byte("answer"), []byte("42"))
	})
	if err != nil {
		t.Error(err)
	}
	//DB populated, lets upload...
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
	err = upLoadCachePartition(path, tf, bucket, true)
	if err != nil {
		t.Error(err)
	}
	//Try to load same partition
	cp, err := newcachepartition(path, bucket)
	if err != nil {
		t.Error(err)
	}
	if cp.lastModified.Before(time.Unix(666, 0)) {
		t.Errorf("Lastmod is too old %v", cp.lastModified)
	}
	v, err := cp.get([]byte("MyBucket"), []byte("answer"))
	if err != nil {
		t.Error(err)
	}
	answer := string(v)
	if answer != "42" {
		t.Errorf("the answer to life the universe and everything is 42, not %v", answer)
	}
	//Test closure
	//Check file really exists
	fname := cp.db.Path()
	_, err = os.Stat(fname)
	if err != nil {
		t.Error(err)
	}
	//Close the db
	err = cp.close()
	if err != nil {
		t.Error(err)
	}
	//Stat again
	_, err = os.Stat(fname)
	if err == nil {
		t.Errorf("file still exists after closing")
	} else if !os.IsNotExist(err) {
		//ignoring file not exists because thats what we want.
		t.Error(err)
	}

}
