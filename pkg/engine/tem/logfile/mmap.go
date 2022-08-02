package logfile

type Mmap struct {
}

func NewMmap(fName string, fsize int64) {

}

func (m *Mmap) Write(b []byte, offset int64) (int, error) {
	return 0, nil
}

func (m *Mmap) Read(offset int64) ([]byte, error) {
	return nil, nil
}

func (m *Mmap) Sync() error {
	return nil
}
