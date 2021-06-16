package web

import "encoding/json"

type ErrResult struct {
	Status int         `json:"status,omitempty"`
	Des    string      `json:"description,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

func toErrResult(status int, des string) []byte {
	b, _ := json.Marshal(&ErrResult{
		Status: status,
		Des:    des,
	})
	return b
}
