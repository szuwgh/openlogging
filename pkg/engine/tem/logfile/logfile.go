package logfile

type LogFile interface {
	Write(b []byte, offset int64) (int, error)

	Read(b []byte, offset int64) (int, error)
	
}
