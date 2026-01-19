REBAR3 ?= $(shell test -e `which rebar3` 2>/dev/null && which rebar3 || echo "./rebar3")

COZODB_TMP_DIR ?= "/tmp/cozodb/"

# RocksDB backend selection:
#   COZODB_BACKEND=rocksdb (default) - Use cozorocks C++ FFI bridge
#   COZODB_BACKEND=newrocksdb        - Use rust-rocksdb crate with env var
#   config
# IMPORTANT: The two backends are MUTUALLY EXCLUSIVE due to allocator conflicts.
COZODB_BACKEND ?= rocksdb

# Cargo feature flags based on selected backend
ifeq ($(COZODB_BACKEND),newrocksdb)
CARGO_FEATURES := --no-default-features --features "new-rocksdb-default"
else
CARGO_FEATURES :=
endif

.PHONY: all
all: build

.PHONY: build
build: cargo-build
	@$(REBAR3) compile

# Build with the selected backend (controlled by COZODB_BACKEND env var)
.PHONY: cargo-build
cargo-build:
	@echo "Building with COZODB_BACKEND=$(COZODB_BACKEND)"
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
