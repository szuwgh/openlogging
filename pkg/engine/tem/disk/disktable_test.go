package disk

import (
	"testing"

	"github.com/szuwgh/temsearch/pkg/engine/tem/cache"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
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
	dir := "./"
	pw, err := newPostingWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
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
			pw:   pw,
			args: args{
				refs: [][]uint64{[]uint64{1, 2, 3, 4}},
			},
		},
	}
	gots := make([]uint64, 0, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.pw.writePosting(tt.args.refs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("postingWriter.writePosting() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == tt.want {
				t.Errorf("postingWriter.writePosting() = %v, want %v", got, tt.want)
			}
			gots = append(gots, got)
		})
	}
	pw.close()
	pr := newPostingReader(dir, &cache.NamespaceGetter{cache.NewCache(cache.NewLRU(defaultSegmentSize * 10)), 1})
	for _, got := range gots {
		ref, refmap := pr.readPosting(got)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(ref)
		t.Log(refmap)
	}
}

func Test_chunkWriter_writeChunks(t *testing.T) {
	dir := "./"
	cw, err := newChunkWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	type args struct {
		b [][]byte
	}
	tests := []struct {
		name    string
		cw      *chunkWriter
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		struct {
			name    string
			cw      *chunkWriter
			args    args
			want    uint64
			wantErr bool
		}{
			name: "test",
			cw:   cw,
			args: args{[][]byte{[]byte("abcdefg123456"), []byte("123456789")}},
		},
	}
	gots := make([]uint64, 0, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cw.writeChunks(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("chunkWriter.writeChunks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == tt.want {
				t.Errorf("chunkWriter.writeChunks() = %v, want %v", got, tt.want)
			}
			gots = append(gots, got)
		})
	}
	cw.close()
	cr := newchunkReader(dir, 0, &cache.NamespaceGetter{cache.NewCache(cache.NewLRU(defaultSegmentSize * 10)), 2})
	for _, got := range gots {
		chunks := cr.ReadChunk(false, got)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range chunks.Bytes() {
			t.Log(string(v))
		}
	}
}
