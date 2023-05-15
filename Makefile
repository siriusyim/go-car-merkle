SHELL=/usr/bin/env bash

all: build
.PHONY: all

unexport GOFLAGS

GOCC?=go

BUILD_DEPS:=

noah: $(BUILD_DEPS)
	rm -f noah
	$(GOCC) build $(GOFLAGS) -o noah ./cmd

.PHONY: noah
BINS+=noah

build: noah
	@[[ $$(type -P "noah") ]] && echo "Caution: you have \
	an existing noah binary in your PATH. you can execute make install to PATH:GOBIN" || true

.PHONY: build

clean:
	rm -rf $(BINS)
.PHONY: clean

install: install-noah

install-noah:
	install -C ./noah $(GOPATH)/bin
