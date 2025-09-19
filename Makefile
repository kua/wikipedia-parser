PROTOC ?= protoc
PROTOC_GEN_GO ?= $(shell which protoc-gen-go)
PROTO_DIR := proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

.PHONY: proto
proto:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative $(PROTO_FILES)

build: proto
	go build -o ./  ./cmd/...

test: proto
	go test -v ./... -run ""
