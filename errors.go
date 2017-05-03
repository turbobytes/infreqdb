package infreqdb

import (
	"net/http"

	"github.com/goamz/goamz/s3"
	"github.com/pkg/errors"
)

var (
	//ErrKeyNotString when partition key is not a valid string.
	ErrKeyNotString = errors.New("Key must be a string")
	//ErrInvalidObject occurs when object in cache is invalid.
	ErrInvalidObject = errors.New("Returned object is incorrect type")
)

//IsNotFound reflects on error and determines if its a real failure or not-found types
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	switch err := errors.Cause(err).(type) {
	case *s3.Error:
		//S3 populates http status code inside the error.
		if err.StatusCode == http.StatusNotFound {
			return true
		}
		// handle specifically
	default:
		// unknown error
	}
	return false
}
