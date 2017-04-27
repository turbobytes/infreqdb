package infreqdb

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"net/http"
	"time"

	"github.com/goamz/goamz/s3"
)

//Partition holds the data for individual partition
type partition struct {
	Items        map[string]interface{}
	lastModified time.Time
}

//loadPartition downloads file from underlying storage and populates memory
func loadPartition(key string, bucket *s3.Bucket) (partition, error) {
	part := partition{
		Items: make(map[string]interface{}),
	}
	//Download file from s3
	//GetResponse just to be able to read Last-Modified
	resp, err := bucket.GetResponse(key)
	if err != nil {
		return part, err
	}
	//Populate last-modified from header
	part.lastModified, err = http.ParseTime(resp.Header.Get("last-modified"))
	if err != nil {
		return part, err
	}
	defer resp.Body.Close()
	//uncompress
	gzrd, err := gzip.NewReader(resp.Body)
	if err != nil {
		return part, err
	}
	defer gzrd.Close()
	dec := gob.NewDecoder(gzrd)
	err = dec.Decode(&part.Items)
	return part, err
}

func upLoadPartition(key string, data map[string]interface{}, bucket *s3.Bucket) error {
	var network bytes.Buffer
	//compress
	gzrw := gzip.NewWriter(&network)
	enc := gob.NewEncoder(gzrw)
	err := enc.Encode(data)
	if err != nil {
		return err
	}
	gzrw.Close()
	return bucket.Put(key, network.Bytes(), "TODO", "", s3.Options{})
}
