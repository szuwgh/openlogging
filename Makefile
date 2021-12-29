# Copyright (c) 2021 szuwgh, Inc.
# Full license can be found in the LICENSE file.

GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean

GO_SOURCE := main.go
GO_BINARY := athena

all: build_go

build_go: $(GO_BINARY)

clean:
	$(GOCLEAN)
	rm -f $(GO_BINARY)

$(GO_BINARY): $(GO_SOURCE)
	$(GOBUILD) -v -o $@