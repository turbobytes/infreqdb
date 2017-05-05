package infreqdb

import (
	"log"
	"time"

	"github.com/bluele/gcache"
	"github.com/boltdb/bolt"
	"github.com/goamz/goamz/s3"
	"github.com/pkg/errors"
)

//DB is an instance of InfreqDB
type DB struct {
	//ttlFunc TTLMethod
	cache   gcache.Cache
	storage Storage
}

func getfname(key interface{}) (string, error) {
	partition, ok := key.(string)
	if !ok {
		return "", ErrKeyNotString
	}
	return partition, nil
}

//New creates a new InfreqDB instance
//len is number of partitions to hold on disk.. use wisely...
//Better to use NewWithStorage() instead. New() will remain for backwards compatibility
func New(bucket *s3.Bucket, prefix string, len int) (*DB, error) {
	return NewWithStorage(&S3Storage{bucket, prefix}, len)
}

//NewWithStorage creates new DB with user provided storage
func NewWithStorage(storage Storage, len int) (*DB, error) {
	gc := gcache.New(len).
		LRU().
		LoaderFunc(func(key interface{}) (interface{}, error) {
			partition, err := getfname(key)
			if err != nil {
				return nil, err
			}
			//Load data from S3 partition
			log.Println("loading", key, partition)
			data, err := newcachepartition(partition, storage)
			if err != nil {
				return nil, err
			}
			return data, nil
		}).
		EvictedFunc(func(k interface{}, v interface{}) {
			//Close the cachepartition when evicting
			part, ok := v.(*cachepartition)
			if ok {
				log.Println("closing", k, part.fname)
				part.close()
			}
		}).
		Build()
	return &DB{
		cache:   gc,
		storage: storage,
	}, nil
}

//Expire evicts the partition from disk
func (db *DB) Expire(partid string) {
	db.cache.Remove(partid)
}

//Silently fails... no evictions on network or parsing failure
func (db *DB) gets3lastmod(partid string) time.Time {
	return db.storage.GetLastMod(partid)
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
			part, ok := v.(*cachepartition)
			if ok {
				//Only check mutable partitions to limit number of HEAD requests
				if part.mutable && part.lastModified.Before(db.gets3lastmod(partid)) {
					count++
					db.Expire(partid)
				}
			}
		}
	}
	return count
}

//Get gets single key from db
func (db *DB) Get(partid string, bucket, key []byte) ([]byte, error) {
	data, err := db.cache.Get(partid)
	if err != nil {
		return nil, err
	}
	cp, ok := data.(*cachepartition)
	if !ok {
		return nil, ErrInvalidObject
	}
	return cp.get(bucket, key)
}

//View inside individual bolt db
//See https://godoc.org/github.com/boltdb/bolt#DB.View for more info
//Second return argument indicates if the partition is mutable.
//Helpful hint for downstream caching.
func (db *DB) View(partid string, fn func(*bolt.Tx) error) (bool, error) {
	data, err := db.cache.Get(partid)
	if err != nil {
		if IsNotFound(err) {
			//Not found errors should not propagate error.
			//Means there is no data for this partition...
			return true, nil
		}
		return true, errors.Wrap(err, "View")
	}
	cp, ok := data.(*cachepartition)
	if !ok {
		return false, ErrInvalidObject
	}
	return cp.mutable, cp.view(fn)
}

//SetPart uploads the partition to S3 and expires local cache
//fname is the path to an uncompressed boltdb file
//Cache for this partition is invalidated. If running on a cluster you need to
// propagate this and Expire(partid) somehow.
// Set mutable to true in case you expect changes to this partition
func (db *DB) SetPart(partid, fname string, mutable bool) error {
	err := db.storage.Put(partid, fname, mutable)
	db.Expire(partid)
	return errors.Wrap(err, "SetPart")
}

//Close closes the db and deletes all local database fragments
func (db *DB) Close() {
	for _, k := range db.cache.Keys() {
		db.cache.Remove(k)
	}
}
