package infreqdb

// work in progress. use bolt on disk instead of map

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/goamz/goamz/s3"
)

type cachepartition struct {
	db           *bolt.DB
	fname        string
	lastModified time.Time
}

func newcachepartition(key string, bucket *s3.Bucket) (*cachepartition, error) {
	cp := &cachepartition{}
	//Download file from s3
	//GetResponse just to be able to read Last-Modified
	resp, err := bucket.GetResponse(key)
	if err != nil {
		return nil, err
	}
	//Populate last-modified from header
	cp.lastModified, err = http.ParseTime(resp.Header.Get("last-modified"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	//uncompress
	gzrd, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer gzrd.Close()
	tmpfile, err := ioutil.TempFile("", "example")
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
	return cp, nil
}
