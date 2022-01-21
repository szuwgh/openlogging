package series

import (
	"github.com/szuwgh/athena/pkg/engine/athena/chunks"
	"github.com/szuwgh/athena/pkg/lib/prometheus/labels"
)

type Series interface {
	GetByID(uint64) (labels.Labels, []chunks.Chunk, error)
}
