REBAR3 ?= $(shell test -e `which rebar3` 2>/dev/null && which rebar3 || echo "./rebar3")

COZODB_TMP_DIR ?= "/tmp/cozodb/"

# Jemalloc compile-time configuration for container compatibility
# This bakes safe defaults directly into the binary - no runtime config needed
# See: https://github.com/tikv/jemallocator/blob/master/jemalloc-sys/README.md
#
# background_thread:false - Prevents crashes in Docker/ECS/container environments
#                           (background threads can fail after fork() in containers)
# dirty_decay_ms:1000     - Balanced memory return to OS
# muzzy_decay_ms:1000     - Balanced memory return to OS
export JEMALLOC_SYS_WITH_MALLOC_CONF ?= background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:1000

# RocksDB backend selection:
#   COZODB_BACKEND=rocksdb (default) - Use cozorocks C++ FFI bridge
#   COZODB_BACKEND=newrocksdb        - Use rust-rocksdb crate with env var
#   config
# IMPORTANT: The two backends are MUTUALLY EXCLUSIVE due to allocator conflicts.
COZODB_BACKEND ?= rocksdb

# io_uring for RocksDB async I/O (Linux-only, requires liburing-dev):
#   COZODB_IO_URING=true  - Enable io_uring (bare-metal Linux recommended)
#   COZODB_IO_URING=false - Disable io_uring (default, safe for containers)
# WARNING: io_uring + jemalloc + RocksDB 10.9.1 crashes in container environments
# (Docker named volumes, AWS ECS) due to TLS conflicts with the BEAM VM.
COZODB_IO_URING ?= false

# Cargo feature flags based on selected backend and options
EXTRA_CARGO_FEATURES :=

ifeq ($(COZODB_IO_URING),true)
EXTRA_CARGO_FEATURES += io-uring
endif

ifeq ($(COZODB_BACKEND),newrocksdb)
ifneq ($(EXTRA_CARGO_FEATURES),)
CARGO_FEATURES := --no-default-features --features "new-rocksdb-default,$(EXTRA_CARGO_FEATURES)"
else
CARGO_FEATURES := --no-default-features --features "new-rocksdb-default"
endif
else
ifneq ($(EXTRA_CARGO_FEATURES),)
CARGO_FEATURES := --features "$(EXTRA_CARGO_FEATURES)"
else
CARGO_FEATURES :=
endif
endif

.PHONY: all
all: build

.PHONY: build
build: cargo-build
	@$(REBAR3) compile

# Build with the selected backend (controlled by COZODB_BACKEND env var)
.PHONY: cargo-build
cargo-build:
	@echo "Building with COZODB_BACKEND=$(COZODB_BACKEND) COZODB_IO_URING=$(COZODB_IO_URING)"
	cd native/cozodb && cargo build --release $(CARGO_FEATURES)
	@mkdir -p priv/crates/cozodb
	@cp native/cozodb/target/release/libcozodb.so priv/crates/cozodb/cozodb.so 2>/dev/null || \
	 cp native/cozodb/target/release/libcozodb.dylib priv/crates/cozodb/cozodb.so 2>/dev/null || \
	 cp native/cozodb/target/release/cozodb.dll priv/crates/cozodb/cozodb.so 2>/dev/null || true

# Convenience targets for specific backends
.PHONY: build-rocksdb
build-rocksdb:
	@COZODB_BACKEND=rocksdb $(MAKE) build

.PHONY: build-newrocksdb
build-newrocksdb:
	@COZODB_BACKEND=newrocksdb $(MAKE) build

.PHONY: deps
deps:
	@$(REBAR3) deps

.PHONY: shell
shell:

	@$(REBAR3) shell

.PHONY: clean
clean:
	@$(REBAR3) cargo clean
	@$(REBAR3) clean

.PHONY: clean-all
clean-all:
	rm -rf $(CURDIR)/priv/crates
	rm -rf $(CURDIR)/_build

.PHONY: distclean
distclean: clean
	@$(REBAR3) clean --all

.PHONY: docs
docs:
	@$(REBAR3) ex_doc

.PHONY: eunit
eunit:
	@$(REBAR3) as test eunit

.PHONY: ct
ct:
	@COZODB_TMP_DIR=$(COZODB_TMP_DIR) ERL_FLAGS="+SDio 128" $(REBAR3) as test ct

.PHONY: benchmark
benchmark:
	@COZODB_TMP_DIR=$(COZODB_TMP_DIR) ERL_FLAGS="+SDio 128" $(REBAR3) as test ct --suite=cozodb_benchmark_SUITE

.PHONY: xref
xref:
	@$(REBAR3) xref


.PHONY: cover
cover:
	@$(REBAR3) cover

.PHONY: proper
proper:
	@$(REBAR3) as test proper


.PHONY: dyalizer
dyalizer:
	@$(REBAR3) dyalizer

.PHONY: test
test: eunit ct

.PHONY: release
release: xref
	@$(REBAR3) as prod release


.PHONY: update-cozo
update-cozo:
	cd native/cozodb && cargo update -p cozo
