package tokenizer

import (
	"errors"
	"fmt"
)

var RegistryInstance *Registry

func Init() {
	RegistryInstance = NewRegistry()
}

//tokenizer
type Tokenizer interface {
	Tokenize(content []byte) Tokens
}

type Constructor func(config map[string]interface{}) (Tokenizer, error)

var registeredConstructors = make(map[string]Constructor)

func RegiterConstructor(_type string, c Constructor) {
	registeredConstructors[_type] = c
}

//tokenizer Registry
type Registry struct {
	tokenizerMap map[string]Constructor
}

func NewRegistry() *Registry {
	ret := &Registry{
		tokenizerMap: make(map[string]Constructor),
	}
	for typ, c := range registeredConstructors {
		ret.RegisterTokenizer(typ, c)
	}
	return ret
}

func (r *Registry) RegisterTokenizer(_type string, constructor Constructor) error {
	_, exist := r.tokenizerMap[_type]
	if exist {
		return errors.New("senderType " + _type + " has been existed")
	}
	r.tokenizerMap[_type] = constructor
	return nil
}

func (r *Registry) NewTokenizer(_type string, config map[string]interface{}) (Tokenizer, error) {
	constructor, exist := r.tokenizerMap[_type]
	if !exist {
		return nil, fmt.Errorf("tokenizer type unsupported : %v", _type)
	}
	tokenizer, err := constructor(config)
	return tokenizer, err
}
