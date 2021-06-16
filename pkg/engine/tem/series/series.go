package series

import (
	"github.com/sophon-lab/temsearch/pkg/engine/tem/chunks"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
)

type Series interface {
	GetByID(uint64) (labels.Labels, []chunks.Chunk, error)
}
