package server

import "github.com/sophon-lab/temsearch/core/indexer/lsm"

//服务
type Server struct {
	//web *Web.Handler
	eg *lsm.Engine
}
