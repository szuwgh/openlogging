package fileutil

import "os"

// OpenDir opens a directory for syncing.
func OpenDir(path string) (*os.File, error) { return os.Open(path) }