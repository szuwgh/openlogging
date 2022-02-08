package series

import (
	"github.com/szuwgh/temsearch/pkg/engine/tem/chunks"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
)

type Series interface {
	GetByID(uint64) (labels.Labels, []chunks.Chunk, error)
}
