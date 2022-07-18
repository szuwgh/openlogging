package logfile

type LogFile interface {
	Write(b []byte, offset int64) (int, error)

	Read(offset int64) ([]byte, error)

	Sync() error
}
