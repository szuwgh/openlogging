package logfile

import "os"

type FileIO struct {
	fd *os.File
}
