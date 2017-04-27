package infreqdb

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bluele/gcache"
	"github.com/goamz/goamz/s3"
)

//DB is an instance of InfreqDB
type DB struct {
	bucket *s3.Bucket
	prefix string
	//ttlFunc TTLMethod
	cache  gcache.Cache
	tmpdir string
}

func getfname(key interface{}) (string, error) {
	partition, ok := key.(string)
	if !ok {
		return "", fmt.Errorf("Key must be a string")
	}
	if strings.Contains(partition, "/") {
		return "", fmt.Errorf("Key must not contain /")
	}
	return partition, nil
}

//New creates a new InfreqDB instance
//len is number of partitions to hold in memory.. use wisely...
func New(bucket *s3.Bucket, prefix string, len int) (*DB, error) {
	gc := gcache.New(len).
		LRU().
		LoaderFunc(func(key interface{}) (interface{}, error) {
			partition, err := getfname(key)
			if err != nil {
				return nil, err
			}
			//TODO: Load data from S3 partition
			data, err := loadPartition(prefix+partition, bucket)
			if err != nil {
				return nil, err
			}
			return data, nil
		}).
		Build()
	return &DB{
		bucket: bucket,
		prefix: prefix,
		//ttlFunc: ttlFunc,
		cache: gc,
	}, nil
}

//Expire evicts the partition from memory
func (db *DB) Expire(partid string) {
	db.cache.Remove(partid)
}

//Silently fails... no evictions on failure
func (db *DB) gets3lastmod(key string) time.Time {
	resp, err := db.bucket.Head(key, map[string][]string{})
	if err != nil {
		log.Println(err)
		return time.Unix(1, 0)
	}
	lmod, err := http.ParseTime(resp.Header.Get("last-modified"))
	if err != nil {
		log.Println(err)
		return time.Unix(1, 0)
	}
	return lmod
}

//CheckExpiry expires items that have changed upstream
//Maybe unexport it and launch as loop
func (db *DB) CheckExpiry() int {
	count := 0
	//TODO: Maybe listing the bucket is more efficient.
	//Loop thru cache and compare last modified, expire if stale
	for k, v := range db.cache.GetALL() {
		partid, ok := k.(string)
		if ok {
			part, ok := v.(partition)
			if ok {
				if part.lastModified.Before(db.gets3lastmod(db.prefix + partid)) {
					count++
					db.Expire(partid)
				}
			}
		}
	}
	return count
}

//Get gets key from db
func (db *DB) Get(partid, key string) (interface{}, error) {
	data, err := db.cache.Get(partid)
	if err != nil {
		return nil, err
	}
	part, ok := data.(partition)
	if !ok {
		return nil, fmt.Errorf("Returned object is incorrect type")
	}
	item, ok := part.Items[key]
	if !ok {
		return nil, fmt.Errorf("Key %v not found in partion %v", key, partid)
	}
	return item, nil
}

//SetPart uploads the partition to S3 and expires local cache
func (db *DB) SetPart(partid string, data map[string]interface{}) error {
	err := upLoadPartition(db.prefix+partid, data, db.bucket)
	db.Expire(partid)
	return err
}

//Close closes the db and deletes all local database fragments
func (db *DB) Close() {
	//db.cache.closeall()
}
