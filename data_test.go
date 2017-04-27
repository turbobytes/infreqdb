package infreqdb

import (
	"testing"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/goamz/goamz/s3/s3test"
)

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

func TestPartitionLoad(t *testing.T) {
	path := "/foo/bar"
	bucket, err := getmockbucket()
	if err != nil {
		t.Error(err)
	}
	mockdata := map[string]interface{}{
		"foo": "bar",
	}
	err = upLoadPartition(path, mockdata, bucket)
	if err != nil {
		t.Error(err)
	}
	part, err := loadPartition(path, bucket)
	if err != nil {
		t.Error(err)
	}
	obj, ok := part.Items["foo"]
	if !ok {
		t.Errorf("key foo not found")
	}
	if obj != "bar" {
		t.Errorf("key foo should be bar, got %v", obj)
	}
	//get invalid partition
	part, err = loadPartition("/something/doesntexist", bucket)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}
