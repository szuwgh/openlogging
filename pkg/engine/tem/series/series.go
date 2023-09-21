package series

import (
	"github.com/szuwgh/hawkobserve/pkg/engine/tem/chunks"
	"github.com/szuwgh/hawkobserve/pkg/lib/prometheus/labels"
)

type Series interface {
	GetByID(uint64) (labels.Labels, []chunks.Chunk, error)
}
