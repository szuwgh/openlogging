package config

import (
	"os"
	"path/filepath"
)

var GOPATH string

func init() {
	GOPATH = os.Getenv("GOPATH")
}

func GetSegment() string {
	return filepath.Join(GOPATH, "src", "data", "k_0")
}
