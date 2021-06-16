package field

type Field interface {
	//域名
	Name() string
	//值
	Value() string
	//域标号
	Number() uint16
	//是否被分词
	IsTokenized() bool
	//是否二进制
	IsBinary() bool
	//是否存储
	IsStore() bool
	//设置number
	SetNumber(uint16)
	//是否被索引
	IsIndex() bool
}
