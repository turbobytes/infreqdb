package infreqdb

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
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

func newcachepartition(part string, storage Storage) (*cachepartition, error) {
	cp := &cachepartition{RWMutex: &sync.RWMutex{}}
	//Download file from storage
	fname, found, mutable, lastmod, err := storage.Get(part)

	if err != nil {
		return nil, err
	}

	//404
	if !found {
		cp.mutable = true
		cp.lastModified = time.Unix(2, 2)
		return cp, nil
	}
	//Ok we have a partition
	//Populate last-modified from header
	cp.lastModified = lastmod
	cp.fname = fname
	st := time.Now()
	cp.db, err = bolt.Open(cp.fname, os.ModeExclusive, &bolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		os.Remove(cp.fname)
		return nil, err
	}
	log.Println("loadedbolt ", part, time.Since(st))
	cp.mutable = mutable
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
