CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -O2 -Isrc -Iinclude
GRPC_CFLAGS := $(shell pkg-config --cflags grpc 2>/dev/null)
GRPC_LIBS := $(shell pkg-config --libs grpc 2>/dev/null)

.PHONY: all test test-proto clean

all: diavasi_consume process

proto_test: src/proto.c src/proto_test.c src/proto.h
	$(CC) $(CFLAGS) -o $@ src/proto.c src/proto_test.c

diavasi_consume: src/proto.c src/consume.c src/example.c
	$(CC) $(CFLAGS) $(GRPC_CFLAGS) -o $@ src/proto.c src/consume.c src/example.c $(GRPC_LIBS)

process: src/proto.c src/consume.c examples/process.c
	$(CC) $(CFLAGS) $(GRPC_CFLAGS) -o $@ src/proto.c src/consume.c examples/process.c $(GRPC_LIBS)

consume_test: src/proto.c src/consume.c src/consume_test.c
	$(CC) $(CFLAGS) $(GRPC_CFLAGS) -o $@ src/proto.c src/consume.c src/consume_test.c $(GRPC_LIBS)

test-proto: proto_test
	./proto_test

test: test-proto
	@if pkg-config --exists grpc; then $(MAKE) consume_test && ./consume_test; else echo "skip: grpc headers are not installed"; fi

clean:
	rm -f proto_test diavasi_consume consume_test process
