package disk

import (
	"github.com/szuwgh/hawkobserve/pkg/lib/furze/gofst"
)

type Builder struct {
	builder *gofst.Builder
}

func NewBuilder() *Builder {
	return &Builder{
		builder: gofst.NewBuilder(),
	}
}

func appendIndex(k []byte, bh blockHandle) error {
	return nil
}

func finishRestarts() {

}

func finishTail() uint32 {
	return 0
}

func reset() {

}

func Get() []byte {
	return nil
}
