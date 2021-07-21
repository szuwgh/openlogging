package disk

import (
	"testing"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/cache.go"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
)

func Test_seriesWriter_addSeries(t *testing.T) {
	dir := "./"
	sw, err := newSeriesWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		isSeries bool
		lset     labels.Labels
		chunks   []ChunkMeta
	}
	tests := []struct {
		name    string
		sw      *seriesWriter
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		struct {
			name    string
			sw      *seriesWriter
			args    args
			want    uint64
			wantErr bool
		}{
			name: "test1",
			sw:   sw,
			args: args{
				isSeries: false,
				lset:     labels.Labels{labels.Label{"zhangshan", "baidu"}, labels.Label{"lisi", "google"}},
				chunks:   []ChunkMeta{ChunkMeta{1, 1626851373, 1626854373}, ChunkMeta{2, 1626851373, 1626856373}},
			},
		},
		struct {
			name    string
			sw      *seriesWriter
			args    args
			want    uint64
			wantErr bool
		}{
			name: "test2",
			sw:   sw,
			args: args{
				isSeries: false,
				lset:     labels.Labels{labels.Label{"wangwu", "ok"}, labels.Label{"wuliu", "yes"}},
				chunks:   []ChunkMeta{ChunkMeta{1, 1626851373, 1626854373}, ChunkMeta{2, 1626851373, 1626856373}},
			},
		},
	}
	gots := make([]uint64, 0, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.sw.addSeries(tt.args.isSeries, tt.args.lset, tt.args.chunks...)
			if (err != nil) != tt.wantErr {
				t.Errorf("seriesWriter.addSeries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == tt.want {
				t.Errorf("seriesWriter.addSeries() = %v, want %v", got, tt.want)
			}
			gots = append(gots, got)
		})
	}
	sw.close()
	sr := newSeriesReader(dir, &cache.NamespaceGetter{cache.NewCache(cache.NewLRU(defaultSegmentSize * 10)), 0})
	for i, got := range gots {
		lset, chunks, err := sr.getByID(got)
		if err != nil {
			t.Fatal(err)
		}
		if labels.Compare(lset, tests[i].args.lset) != 0 {
			t.Fatal("seriesWriter.addSeries() labels not equal", lset, tests[i].args.lset)
		}
		for j, c := range chunks {
			if !isEqualChunk(c, tests[i].args.chunks[j]) {
				t.Fatal("seriesWriter.addSeries() chunks not equal", c, tests[i].args.chunks[j])
			}
		}
	}
	sr.release()
}

func Test_postingWriter_writePosting(t *testing.T) {
	type args struct {
		refs [][]uint64
	}
	tests := []struct {
		name    string
		pw      *postingWriter
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		struct {
			name    string
			pw      *postingWriter
			args    args
			want    uint64
			wantErr bool
		}{
			name: "test1",
			pw:   sw,
			args: args{
				isSeries: false,
				lset:     labels.Labels{labels.Label{"zhangshan", "baidu"}, labels.Label{"lisi", "google"}},
				chunks:   []ChunkMeta{ChunkMeta{1, 1626851373, 1626854373}, ChunkMeta{2, 1626851373, 1626856373}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.pw.writePosting(tt.args.refs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("postingWriter.writePosting() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("postingWriter.writePosting() = %v, want %v", got, tt.want)
			}
		})
	}
}
