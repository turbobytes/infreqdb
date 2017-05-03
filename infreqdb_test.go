package infreqdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/goamz/goamz/s3/s3test"
)

func gettmpfile(t *testing.T) string {
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		t.Error(t)
	}
	tmpfile.Close()
	fname := tmpfile.Name()
	err = os.Remove(fname)
	if err != nil {
		t.Error(t)
	}
	return fname
}

func getmockbucket() (*s3.Bucket, error) {
	srv, err := s3test.NewServer(&s3test.Config{})
	if err != nil {
		return nil, err
	}
	region := aws.Region{
		Name:                 "test",
		S3Endpoint:           srv.URL(),
		S3LocationConstraint: true, // s3test server requires a LocationConstraint
		//Sign:                 aws.SignV2,
	}
	auth := aws.Auth{}
	s := s3.New(auth, region)
	bucket := s.Bucket("foo")
	err = bucket.PutBucket(s3.Private)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func TestDB(t *testing.T) {
	bucket, err := getmockbucket()
	if err != nil {
		t.Error(err)
	}
	db, err := New(bucket, "/foo/", 200)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	//Test err key does not exist
	_, err = db.Get("whatever", []byte("foo"), []byte("bar"))
	if err == nil {
		t.Error("Should have error, got none")
	}
	//Populate a local file...
	tf := gettmpfile(t)
	defer os.Remove(tf)
	bdb, err := bolt.Open(tf, 0600, nil)
	if err != nil {
		t.Error(err)
	}
	err = bdb.Update(func(tx *bolt.Tx) error {
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
	err = bdb.Close()
	if err != nil {
		t.Error(err)
	}

	err = db.SetPart("whatever", tf, true)
	if err != nil {
		t.Error(err)
	}
	//Try to get same partition
	item, err := db.Get("whatever", []byte("MyBucket"), []byte("answer"))
	if err != nil {
		t.Error(err)
	}
	if string(item) != "42" {
		t.Errorf("expected 42, got %s", item)
	}
	//Run CheckExpiry() loop and make sure nothing is expired
	count := db.CheckExpiry()
	if count != 0 {
		t.Errorf("expected 0 expires, got %v", count)
	}
	//YIKES: Sleep a second since http time is at 1 second resolution
	time.Sleep(time.Second)
	//Create bolt database again
	tf = gettmpfile(t)
	defer os.Remove(tf)
	bdb, err = bolt.Open(tf, 0600, nil)
	if err != nil {
		t.Error(err)
	}
	err = bdb.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucket([]byte("MyBucket"))
		if e != nil {
			return fmt.Errorf("create bucket: %s", e)
		}
		return b.Put([]byte("question"), []byte("What do you get if you multiply six by nine?"))
	})
	if err != nil {
		t.Error(err)
	}
	//DB populated, lets upload...
	err = bdb.Close()
	if err != nil {
		t.Error(err)
	}

	err = db.SetPart("whatever", tf, true)
	if err != nil {
		t.Error(err)
	}

	//Confirm new value is in local database
	item, err = db.Get("whatever", []byte("MyBucket"), []byte("question"))
	if err != nil {
		t.Error(err)
	}
	if string(item) != "What do you get if you multiply six by nine?" {
		t.Errorf("expected question, got %s", item)
	}

	//Test old answer is missing since we replaced the database
	_, err = db.Get("whatever", []byte("MyBucket"), []byte("answer"))
	if err == nil {
		t.Error("Expected an error")
	}
	//Run CheckExpiry() loop and make sure nothing has expired
	count = db.CheckExpiry()
	if count != 0 {
		t.Errorf("expected 0 expires, got %v", count)
	}
	//YIKES: Sleep a second since http time is at 1 second resolution
	time.Sleep(time.Second)
	//manually sideload new data, asif some other process or server changed the object
	tf = gettmpfile(t)
	defer os.Remove(tf)
	bdb, err = bolt.Open(tf, 0600, nil)
	if err != nil {
		t.Error(err)
	}
	err = bdb.Update(func(tx *bolt.Tx) error {
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
	err = bdb.Close()
	if err != nil {
		t.Error(err)
	}

	err = upLoadCachePartition("/foo/whatever", tf, bucket, true)
	if err != nil {
		t.Error(err)
	}
	//Ensure expiry function catches the change
	count = db.CheckExpiry()
	if count != 1 {
		t.Errorf("expected 1 expires, got %v", count)
	}
	//Ensure fresh value is available
	item, err = db.Get("whatever", []byte("MyBucket"), []byte("answer"))
	if err != nil {
		t.Error(err)
	}
	if string(item) != "42" {
		t.Errorf("expected 42, got %s", item)
	}
}
