package field

type FieldText struct {
	name    string
	value   []byte
	number  uint16
	isStore bool
}

func NewFieldText(name string, value []byte) *FieldText {
	fieldText := &FieldText{
		name:  name,
		value: value,
	}
	return fieldText
}

func (f *FieldText) Name() string {
	return f.name
}

func (f *FieldText) Value() string {
	return string(f.value)
}

func (f *FieldText) Number() uint16 {
	return f.number
}

func (f *FieldText) IsTokenized() bool {
	return true
}

func (f *FieldText) IsBinary() bool {
	return false
}

func (f *FieldText) IsStore() bool {
	return true
}

func (f *FieldText) SetNumber(num uint16) {
	f.number = num
}

func (f *FieldText) SetStore(isStore bool) {
	f.isStore = isStore
}

func (f *FieldText) IsIndex() bool {
	return true
}
