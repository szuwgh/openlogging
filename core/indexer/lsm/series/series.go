package series

import (
	"github.com/sophon-lab/temsearch/core/indexer/lsm/chunks"
	"github.com/sophon-lab/temsearch/core/indexer/lsm/labels"
)

type Series interface {
	GetByID(uint64) (labels.Labels, []chunks.Chunk, error)
}
