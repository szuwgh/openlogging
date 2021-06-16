package search

//查询语句
type QueryESL struct {
	Tags        map[string]string
	Term        []string
	Opt         string
	MinTime     int64
	MaxTime     int64
	TimeIsOrder bool //时间正序 倒叙
}

//{"Tags":{"job":"wo2"},"Term":["a"],"Opt":"and"}
