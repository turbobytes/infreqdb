package infreqdb

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/goamz/goamz/s3"
)

//Storage allows various operations against an object store.
//Use any object/file store.
//The Storage is responsible for [un]compression.
type Storage interface {
	//Get retrieves a partition file from object store
	Get(part string) (fname string, found, mutable bool, lastmod time.Time, err error)
	//Put stores partition into object store
	Put(part, fname string, mutable bool) error
	//GetLastMod gets the last modified time for a partition.
	GetLastMod(part string) time.Time
}

//S3Storage impliments interface to access AWS S3.
//Uses gzip for compression
type S3Storage struct {
	bucket *s3.Bucket
	prefix string
}

//NewS3Storage creates new storage that talks to aws S3
func NewS3Storage(bucket *s3.Bucket, prefix string) *S3Storage {
	return &S3Storage{bucket, prefix}
}

//key returns the S3 key for partition
func (s3s *S3Storage) key(part string) string {
	return s3s.prefix + part
}

//Get a partition file from S3 store into local file, supress not found error
func (s3s *S3Storage) Get(part string) (fname string, found, mutable bool, lastmod time.Time, err error) {
	//Access s3
	resp, err := s3s.bucket.GetResponse(s3s.key(part))
	if err != nil {
		if IsNotFound(err) {
			//Way back, but still in future for error condition in GetLastMod
			lastmod = time.Unix(2, 2)
			//Flag it as mutable so on future Expire() loop we check again
			mutable = true
			//Do not propagate not founc error
			err = nil
		}
		//Return... with or without error
		return
	}
	lastmod, err = s3s.parselmod(resp.Header.Get("last-modified"))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	gzrd, err := gzip.NewReader(resp.Body)
	if err != nil {
		return
	}
	defer gzrd.Close()
	//The location of the TempFile is totally up to the Storage implimentation
	tmpfile, err := ioutil.TempFile("", "infreqdb-")
	if err != nil {
		return
	}
	_, err = io.Copy(tmpfile, gzrd)
	if err != nil {
		return
	}
	fname = tmpfile.Name()
	tmpfile.Close()
	if err != nil {
		os.Remove(fname)
		return
	}
	//All is well... populate mutable and found
	mutable = resp.Header.Get("x-amz-meta-mutable") != ""
	found = true
	return
}

//Put uploads a partition to s3
func (s3s *S3Storage) Put(part, fname string, mutable bool) error {
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
	return s3s.bucket.PutHeader(s3s.key(part), network.Bytes(), hdr, "")
}

//parselmod parses last-modified string into time
func (s3s *S3Storage) parselmod(t string) (time.Time, error) {
	lmod, err := http.ParseTime(t)
	if err != nil {
		//s3test has issues, lets workaround it.
		//https://github.com/goamz/goamz/issues/137
		lmod, err = time.Parse("Mon, 2 Jan 2006 15:04:05 GMT", t)
	}
	return lmod, err
}

//GetLastMod gets last modification time for a partition
//Return ancient time on failure
func (s3s *S3Storage) GetLastMod(part string) time.Time {
	resp, err := s3s.bucket.Head(s3s.key(part), map[string][]string{})
	if err != nil {
		log.Println(err)
		return time.Unix(1, 0)
	}
	lmod, err := s3s.parselmod(resp.Header.Get("last-modified"))
	if err != nil {
		return time.Unix(1, 0)
	}
	return lmod
}
