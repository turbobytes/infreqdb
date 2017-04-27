package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/onrik/logrus/filename"
	"github.com/turbobytes/infreqdb"
)

var (
	prefix       = flag.String("prefix", "/infreqdb-example/", "prefix for all s3 keys")
	s3bucketName = flag.String("s3bucket", "", "S3 bucket name to use")
	s3region     = flag.String("s3region", "", "S3 bucket name to use")
	prefill      = flag.Bool("prefill", false, "Prefill data in s3")
	s3bucket     *s3.Bucket
	db           *infreqdb.DB
	tmpdir       string
	cities       = []string{"bangkok", "singapore", "new york", "amsterdam"}
)

//CityInfo holds demo data for a particular hour
type CityInfo struct {
	Temperature float64
	WindSpeed   float64
}

func randCityInfo() CityInfo {
	return CityInfo{rand.Float64(), rand.Float64()}
}

func decodeCityInfo(b []byte) CityInfo {
	ci := CityInfo{}
	network := bytes.NewBuffer(b)
	dec := gob.NewDecoder(network)
	err := dec.Decode(&ci)
	if err != nil {
		log.Fatal(err)
	}
	return ci
}

func (ci CityInfo) marshal() ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(&ci)
	if err != nil {
		return nil, err
	}
	return network.Bytes(), nil
}

func init() {
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
	flag.Parse()
	if *s3bucketName == "" {
		log.Fatal("Bucket name must be provided")
	}
	if *s3region == "" {
		log.Fatal("Region name must be provided")
	}
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}
	region, ok := aws.Regions[*s3region]
	if !ok {
		log.Fatalf("Region %v not found", *s3region)
	}

	s := s3.New(auth, region)
	s3bucket = s.Bucket(*s3bucketName)
	db, err = infreqdb.New(s3bucket, *prefix, 100)
	if err != nil {
		log.Fatal(err)
	}
}

func generatedb(t time.Time) {
	partid := t.Format("2006-01-02")
	log.Println("Creating partition for ", partid)
	fname := fmt.Sprintf("%s/%s", tmpdir, partid)
	defer os.Remove(fname)
	log.Println("fname ", fname)
	bdb, err := bolt.Open(fname, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	end := t.Add(time.Hour * 24)
	for _, city := range cities {
		//create buckets
		tx, err := bdb.Begin(true)
		if err != nil {
			log.Fatal("begin", err)
		}

		b, err := tx.CreateBucket([]byte(city))
		if err != nil {
			log.Fatal("createbucket", err)
		}
		cur := t
		var bin, payload []byte
		for cur.Before(end) {
			bin, err = cur.MarshalBinary()
			if err != nil {
				log.Fatal(err)
			}
			payload, err = randCityInfo().marshal()
			if err != nil {
				log.Fatal(err)
			}
			err = b.Put(bin, payload)
			if err != nil {
				log.Fatal(err)
			}
			cur = cur.Add(time.Minute)
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal("commit", err)
		}
	}

	err = bdb.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Uploading")
	db.SetPart(partid, fname)
}

func prefildb() {
	//Initialize and upload data
	start, err := time.Parse("2006-01-02T15:04:05", "2017-01-01T00:00:00")
	if err != nil {
		log.Fatal(err)
	}
	end, err := time.Parse("2006-01-02T15:04:05", "2017-01-31T23:59:59")
	if err != nil {
		log.Fatal(err)
	}
	tmpdir, err = ioutil.TempDir("", "example-")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(tmpdir) // clean up

	for start.Before(end) {
		generatedb(start)
		start = start.Add(time.Hour * 24)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	defer db.Close()
	if *prefill {
		prefildb()
	}
	//Example of reading data
	ts, err := time.Parse("2006-01-02T15:04:05", "2017-01-01T00:01:00")
	if err != nil {
		log.Fatal(err)
	}
	bin, err := ts.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	//Get value of partition 2017-01-01 , bucket bangkok and key 2017-01-01T00:01:00
	v, err := db.Get("2017-01-01", []byte("bangkok"), bin)
	if err != nil {
		log.Fatal(err)
	}
	//Decode and print
	log.Println(decodeCityInfo(v))

	//Which was the hottest city at 2017-01-15T00:02:00
	ts, err = time.Parse("2006-01-02T15:04:05", "2017-01-15T00:02:00")
	if err != nil {
		log.Fatal(err)
	}
	bin, err = ts.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	hottest := ""
	hottestTemp := 0.0
	//Using View... proxy to bolt.DB.View
	db.View("2017-01-15", func(tx *bolt.Tx) error {
		for _, city := range cities {
			b := tx.Bucket([]byte(city))
			v := b.Get(bin)
			ci := decodeCityInfo(v)
			if ci.Temperature > hottestTemp {
				hottest = city
				hottestTemp = ci.Temperature
			}
		}
		return nil
	})
	fmt.Printf("Hottest city was %v with temperature %v\n", hottest, hottestTemp)
}
