package disk

import (
	"fmt"
	"testing"

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
		{
			name: "test1",
			sw:   sw,
			args: args{
				isSeries: false,
				lset:     labels.Labels{labels.Label{"zhangshan", "baidu"}, labels.Label{"lisi", "google"}},
				chunks:   []ChunkMeta{ChunkMeta{1, 1626851373, 1626854373}, ChunkMeta{2, 1626851373, 1626856373}},
			},
		},
		{
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
			t.Log("ref", got)
			// if got == tt.want {
			// 	t.Errorf("seriesWriter.addSeries() = %v, want %v", got, tt.want)
			// }
			gots = append(gots, got)
		})
	}
	sw.close()
	sr, err := newSeriesReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, got := range gots {
		lset, chunks, err := sr.getByID(got)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(lset, chunks)
		if labels.Compare(lset, tests[i].args.lset) != 0 {
			t.Fatal("seriesWriter.addSeries() labels not equal", lset, tests[i].args.lset)
		}
		for j, c := range chunks {
			if !isEqualChunk(c, tests[i].args.chunks[j]) {
				t.Fatal("seriesWriter.addSeries() chunks not equal", c, tests[i].args.chunks[j])
			}
		}
	}
	sr.close()
}

func Test_postingWriter_writePosting(t *testing.T) {
	dir := "./"
	pw, err := newSeriesWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		refs [][]uint64
	}

	tests := []struct {
		name    string
		pw      *seriesWriter
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		{
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
			// if got == tt.want {
			// 	t.Errorf("postingWriter.writePosting() = %v, want %v", got, tt.want)
			// }
			gots = append(gots, got)
		})
	}
	pw.close()
	pr, _ := newSeriesReader(dir)
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
	cw, err := newSeriesWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	type args struct {
		b [][]byte
	}
	tests := []struct {
		name    string
		cw      *seriesWriter
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
		{
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
			// if got == tt.want {
			// 	t.Errorf("chunkWriter.writeChunks() = %v, want %v", got, tt.want)
			// }
			gots = append(gots, got)
		})
	}
	cw.close()
	cr, _ := newSeriesReader(dir)
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

func Test_KeyWriter(t *testing.T) {
	var buf [48]byte
	dir := "./"
	kw, err := newKeyWriter(dir, buf[:])
	if err != nil {
		t.Fatal(err)
	}
	kw.setTagName([]byte("name"))
	//kw.add([]byte("aaa"), []byte("111"))
	kw.add([]byte("aa1"), []byte("22"))
	kw.add([]byte("aa2"), []byte("33"))
	kw.add([]byte("aa3"), []byte("44"))
	kw.add([]byte("aa4"), []byte("55"))
	kw.add([]byte("aa5"), []byte("111"))
	kw.add([]byte("aa6"), []byte("22"))
	kw.add([]byte("aa7"), []byte("33"))
	kw.add([]byte("aa8"), []byte("44"))
	kw.add([]byte("aa9"), []byte("55"))
	kw.finishTag()
	kw.close()

	ir, err := newKeyReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	ir.print("name")
	res := ir.find("name", []byte("aa7"))
	fmt.Println(string(res))
}

func Test_Pos(t *testing.T) {
	var offset uint64 = 500
	var length uint64 = 65535
	n := offset<<16 | length
	fmt.Println("n:", n)

	offset = n >> 16
	length = uint64((n << 48) >> 48)
	fmt.Println("offset:", offset, "length:", length)
}
