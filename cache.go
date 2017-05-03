package infreqdb

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/goamz/goamz/s3"
)

type cachepartition struct {
	*sync.RWMutex
	db           *bolt.DB
	fname        string
	lastModified time.Time
	mutable      bool
}

func (cp *cachepartition) view(fn func(*bolt.Tx) error) error {
	//Locking... so we when we close bolt.DB there are no reads inflight
	if cp.db == nil {
		//cp would be nil if the partition did not exist
		return nil
	}
	cp.RLock()
	defer cp.RUnlock()
	return cp.db.View(fn)
}

func (cp *cachepartition) get(bucket, key []byte) (v []byte, err error) {
	err = cp.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("Bucket %s not found", bucket)
		}
		v = b.Get(key)
		if v == nil {
			return fmt.Errorf("Key %v not found in bucket %v", key, bucket)
		}
		return nil
	})
	return
}

func (cp *cachepartition) close() error {
	//Lock forever... no more reads here...
	//Lock waits for all readers to finish...
	cp.Lock()
	if cp.fname != "" {
		defer os.Remove(cp.fname)
	}
	if cp.db != nil {
		return cp.db.Close()
	}
	return nil
}

func newcachepartition(key string, bucket *s3.Bucket) (*cachepartition, error) {
	cp := &cachepartition{RWMutex: &sync.RWMutex{}}
	//Download file from s3
	//GetResponse just to be able to read Last-Modified
	resp, err := bucket.GetResponse(key)
	//Handle notfound errors differently
	if err != nil {
		if IsNotFound(err) {
			cp.mutable = true
			cp.lastModified = time.Unix(2, 2)
			return cp, nil
		}
		return nil, err
	}
	//Populate last-modified from header
	cp.lastModified, err = http.ParseTime(resp.Header.Get("last-modified"))
	if err != nil {
		//s3test has issues, lets workaround it.
		//https://github.com/goamz/goamz/issues/137
		cp.lastModified, err = time.Parse("Mon, 2 Jan 2006 15:04:05 GMT", resp.Header.Get("last-modified"))
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()
	//uncompress
	gzrd, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer gzrd.Close()
	tmpfile, err := ioutil.TempFile("", "infreqdb-")
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(tmpfile, gzrd)
	cp.fname = tmpfile.Name()
	tmpfile.Close()
	if err != nil {
		os.Remove(cp.fname)
		return nil, err
	}
	cp.db, err = bolt.Open(cp.fname, os.ModeExclusive, nil)
	if err != nil {
		os.Remove(cp.fname)
		return nil, err
	}
	//TODO: Until we can detect it.
	cp.mutable = resp.Header.Get("x-amz-meta-mutable") != ""
	return cp, nil
}

func upLoadCachePartition(key, fname string, bucket *s3.Bucket, mutable bool) error {
	var network bytes.Buffer
	//compress..
	//Yikes in memory
	gzrw := gzip.NewWriter(&network)
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(gzrw, f)
	if err != nil {
		return err
	}
	gzrw.Close()
	hdr := make(http.Header)
	if mutable {
		hdr.Set("x-amz-meta-mutable", "yes")
	}
	return bucket.PutHeader(key, network.Bytes(), hdr, "")
}
