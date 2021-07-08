package server

import (
	"github.com/sophon-lab/temsearch/pkg/engine/tem"
)

//服务
type Server struct {
	eg *tem.Engine
}

func (s *Server) Index(b []byte) error {
	err := s.eg.Index(b)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Search(input string) {

}
