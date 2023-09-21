# Copyright (c) 2021 szuwgh, Inc.
# Full license can be found in the LICENSE file.

GOCMD := go
RUSTCMD := cargo build
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean


GO_SOURCE := main.go
GO_BINARY := hawkobserve

all: build_go

build_go: $(GO_BINARY)

clean:
	$(GOCLEAN)
	rm -f $(GO_BINARY)

build:
	cd pkg/lib/furze && cargo build
	$(GOBUILD) -v -o $(GO_BINARY)  