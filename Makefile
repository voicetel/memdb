# Makefile for memdb — in-memory SQLite with persistence, replication, and a Postgres wire server

SHELL := /bin/bash

# ============================================================================
# VARIABLES
# ============================================================================

BINARY_NAME    := memdb
GO             := go
BUILD_DIR      := ./build
CMD_DIR        := ./cmd/memdb
INSTALL_DIR    := /usr/local/bin

# Test variables
TEST_TIMEOUT       := 60s
TEST_TIMEOUT_LONG  := 120s
COVERAGE_DIR       := ./coverage
COVERAGE_FILE      := $(COVERAGE_DIR)/coverage.out
COVERAGE_HTML      := $(COVERAGE_DIR)/coverage.html
BENCH_FILE         := $(COVERAGE_DIR)/bench.txt
COVERAGE_THRESHOLD := 70

# Version information
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S UTC')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build flags
LDFLAGS := -ldflags "-s -w \
	-X 'main.Version=$(VERSION)' \
	-X 'main.BuildTime=$(BUILD_TIME)' \
	-X 'main.GitCommit=$(GIT_COMMIT)'"

# Detect OS and architecture
GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

# Linter
GOLANGCI_LINT := $(shell which golangci-lint 2>/dev/null || echo "$(HOME)/go/bin/golangci-lint")

# Colors
RED     := \033[0;31m
GREEN   := \033[0;32m
YELLOW  := \033[1;33m
BLUE    := \033[0;34m
CYAN    := \033[0;36m
MAGENTA := \033[0;35m
NC      := \033[0m

# ============================================================================
# DEFAULT
# ============================================================================

.DEFAULT_GOAL := help

.PHONY: all
all: clean lint test build ## Clean, lint, test, and build

# ============================================================================
# DIRECTORIES
# ============================================================================

.PHONY: dirs
dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(COVERAGE_DIR)

# ============================================================================
# BUILD
# ============================================================================

.PHONY: build
build: dirs ## Build the memdb CLI binary
	@echo -e "$(BLUE)Building $(BINARY_NAME)...$(NC)"
	@echo "  Version : $(VERSION)"
	@echo "  OS/Arch : $(GOOS)/$(GOARCH)"
	@echo "  Commit  : $(GIT_COMMIT)"
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ Build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

.PHONY: build-prod
build-prod: dirs ## Build optimised production binary (stripped)
	@echo -e "$(BLUE)Building production $(BINARY_NAME)...$(NC)"
	CGO_ENABLED=1 $(GO) build $(LDFLAGS) \
		-o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ Production build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"
	@echo "  Size: $$(du -h $(BUILD_DIR)/$(BINARY_NAME) | cut -f1)"

.PHONY: build-all
build-all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 ## Build for all supported platforms
	@echo -e "$(GREEN)✓ All platform builds complete$(NC)"

.PHONY: build-linux-amd64
build-linux-amd64: dirs
	@echo -e "$(BLUE)Building for linux/amd64...$(NC)"
	@mkdir -p $(BUILD_DIR)/linux-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 \
		$(GO) build $(LDFLAGS) \
		-o $(BUILD_DIR)/linux-amd64/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ linux/amd64 done$(NC)"

.PHONY: build-linux-arm64
build-linux-arm64: dirs
	@echo -e "$(BLUE)Building for linux/arm64...$(NC)"
	@mkdir -p $(BUILD_DIR)/linux-arm64
	GOOS=linux GOARCH=arm64 CGO_ENABLED=1 \
		$(GO) build $(LDFLAGS) \
		-o $(BUILD_DIR)/linux-arm64/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ linux/arm64 done$(NC)"

.PHONY: build-darwin-amd64
build-darwin-amd64: dirs
	@echo -e "$(BLUE)Building for darwin/amd64...$(NC)"
	@mkdir -p $(BUILD_DIR)/darwin-amd64
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 \
		$(GO) build $(LDFLAGS) \
		-o $(BUILD_DIR)/darwin-amd64/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ darwin/amd64 done$(NC)"

.PHONY: build-darwin-arm64
build-darwin-arm64: dirs
	@echo -e "$(BLUE)Building for darwin/arm64 (Apple Silicon)...$(NC)"
	@mkdir -p $(BUILD_DIR)/darwin-arm64
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=1 \
		$(GO) build $(LDFLAGS) \
		-o $(BUILD_DIR)/darwin-arm64/$(BINARY_NAME) $(CMD_DIR)
	@echo -e "$(GREEN)✓ darwin/arm64 done$(NC)"

# ============================================================================
# RUN
# ============================================================================

.PHONY: run
run: build ## Build and run the memdb server with default options
	@echo -e "$(BLUE)Starting $(BINARY_NAME) serve...$(NC)"
	$(BUILD_DIR)/$(BINARY_NAME) serve

.PHONY: run-args
run-args: build ## Run with custom args — usage: make run-args ARGS="serve --file /tmp/x.db"
	@if [ -z "$(ARGS)" ]; then \
		echo -e "$(RED)Error: ARGS not set. Example: make run-args ARGS=\"serve --file /tmp/x.db\"$(NC)"; \
		exit 1; \
	fi
	$(BUILD_DIR)/$(BINARY_NAME) $(ARGS)

# ============================================================================
# DEPENDENCIES
# ============================================================================

.PHONY: deps
deps: ## Download and tidy module dependencies
	@echo -e "$(BLUE)Tidying dependencies...$(NC)"
	$(GO) mod download
	$(GO) mod tidy
	@echo -e "$(GREEN)✓ Dependencies up to date$(NC)"

.PHONY: deps-upgrade
deps-upgrade: ## Upgrade all dependencies to latest compatible versions
	@echo -e "$(BLUE)Upgrading dependencies...$(NC)"
	$(GO) get -u ./...
	$(GO) mod tidy
	@echo -e "$(GREEN)✓ Dependencies upgraded$(NC)"

.PHONY: tools
tools: ## Install development tools (golangci-lint, benchstat, gosec)
	@echo -e "$(BLUE)Installing development tools...$(NC)"
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest || \
		echo -e "$(YELLOW)Warning: could not install golangci-lint$(NC)"
	$(GO) install golang.org/x/perf/cmd/benchstat@latest || \
		echo -e "$(YELLOW)Warning: could not install benchstat$(NC)"
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@latest || \
		echo -e "$(YELLOW)Warning: could not install gosec$(NC)"
	@echo -e "$(GREEN)✓ Tools installed$(NC)"

# ============================================================================
# CORE TEST TARGETS
# ============================================================================

.PHONY: test
test: dirs ## Run all tests
	@echo -e "$(BLUE)Running all tests...$(NC)"
	$(GO) test -v -timeout $(TEST_TIMEOUT) ./...
	@echo -e "$(GREEN)✓ All tests passed$(NC)"

.PHONY: test-race
test-race: dirs ## Run all tests with the race detector
	@echo -e "$(BLUE)Running all tests with race detector...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) ./...
	@echo -e "$(GREEN)✓ No race conditions detected$(NC)"

.PHONY: test-short
test-short: dirs ## Run only short tests (skips slow integration paths)
	@echo -e "$(BLUE)Running short tests...$(NC)"
	$(GO) test -short -v -timeout $(TEST_TIMEOUT) ./...
	@echo -e "$(GREEN)✓ Short tests passed$(NC)"

.PHONY: test-coverage
test-coverage: dirs ## Run all tests and generate an HTML coverage report
	@echo -e "$(BLUE)Running tests with coverage...$(NC)"
	$(GO) test -race -timeout $(TEST_TIMEOUT) \
		-coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo -e "$(GREEN)✓ Coverage report: $(COVERAGE_HTML)$(NC)"
	@echo -e "$(YELLOW)Coverage summary:$(NC)"
	@$(GO) tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print "  Total: " $$3}'

.PHONY: test-coverage-check
test-coverage-check: test-coverage ## Fail if total coverage is below COVERAGE_THRESHOLD (default 70%)
	@echo -e "$(BLUE)Checking coverage threshold ($(COVERAGE_THRESHOLD)%)...$(NC)"
	@TOTAL=$$($(GO) tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print $$3}' | tr -d '%'); \
	if [ $$(echo "$$TOTAL < $(COVERAGE_THRESHOLD)" | bc) -eq 1 ]; then \
		echo -e "$(RED)✗ Coverage $$TOTAL% is below threshold $(COVERAGE_THRESHOLD)%$(NC)"; \
		exit 1; \
	fi
	@echo -e "$(GREEN)✓ Coverage threshold met$(NC)"

.PHONY: test-specific
test-specific: dirs ## Run a specific test — usage: make test-specific TEST=TestName
	@if [ -z "$(TEST)" ]; then \
		echo -e "$(RED)Error: TEST not set. Usage: make test-specific TEST=TestName$(NC)"; \
		exit 1; \
	fi
	@echo -e "$(BLUE)Running test: $(TEST)...$(NC)"
	$(GO) test -v -race -timeout $(TEST_TIMEOUT) -run $(TEST) ./...

.PHONY: test-count
test-count: dirs ## Print a count of passing tests
	@echo -e "$(BLUE)Counting passing tests...$(NC)"
	@$(GO) test -v -count=1 ./... 2>&1 | grep -c "^--- PASS" | \
		xargs -I{} echo -e "$(GREEN)✓ {} tests passed$(NC)"

# ============================================================================
# PACKAGE-LEVEL TEST TARGETS
# ============================================================================

.PHONY: test-core
test-core: dirs ## Run tests for the root memdb package only
	@echo -e "$(CYAN)Running core memdb tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) github.com/voicetel/memdb
	@echo -e "$(GREEN)✓ Core tests passed$(NC)"

.PHONY: test-backends
test-backends: dirs ## Run tests for the backends package
	@echo -e "$(CYAN)Running backends tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) ./backends/...
	@echo -e "$(GREEN)✓ Backends tests passed$(NC)"

.PHONY: test-server
test-server: dirs ## Run tests for the server package (PostgreSQL wire protocol)
	@echo -e "$(CYAN)Running server tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) ./server/...
	@echo -e "$(GREEN)✓ Server tests passed$(NC)"

.PHONY: test-replication
test-replication: dirs ## Run tests for the replication package (leader/follower)
	@echo -e "$(CYAN)Running replication tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) ./replication/...
	@echo -e "$(GREEN)✓ Replication tests passed$(NC)"

.PHONY: test-raft
test-raft: dirs ## Run tests for the Raft consensus FSM
	@echo -e "$(CYAN)Running Raft FSM tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT_LONG) ./replication/raft/...
	@echo -e "$(GREEN)✓ Raft tests passed$(NC)"

.PHONY: test-wal
test-wal: dirs ## Run WAL-specific tests only
	@echo -e "$(CYAN)Running WAL tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) -run "TestWAL" ./...
	@echo -e "$(GREEN)✓ WAL tests passed$(NC)"

.PHONY: test-durability
test-durability: dirs ## Run durability mode tests (DurabilityNone, WAL, Sync)
	@echo -e "$(CYAN)Running durability tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) -run "TestDurability" ./...
	@echo -e "$(GREEN)✓ Durability tests passed$(NC)"

.PHONY: test-replica
test-replica: dirs ## Run read replica pool tests
	@echo -e "$(CYAN)Running replica pool tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) -run "TestReplica" ./...
	@echo -e "$(GREEN)✓ Replica pool tests passed$(NC)"

.PHONY: test-replication-integrity
test-replication-integrity: dirs ## Run end-to-end replication integrity tests (3-node cluster, convergence, failover)
	@echo -e "$(CYAN)Running replication integrity tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT_LONG) -run "TestReplication_" ./replication/raft/...
	@echo -e "$(GREEN)✓ Replication integrity tests passed$(NC)"

.PHONY: test-replication-soak
test-replication-soak: dirs ## Run the high-volume replication soak test with race detector
	@echo -e "$(CYAN)Running replication soak test (HighVolume_NoCorruption, 10k-entry Snapshot)...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT_LONG) \
		-run "TestReplication_HighVolume_NoCorruption|TestReplication_Snapshot_RestoresCleanly" \
		./replication/raft/...
	@echo -e "$(GREEN)✓ Replication soak test passed$(NC)"

.PHONY: test-profiling
test-profiling: dirs ## Run the profiling package tests (HTTP pprof server + capture helpers)
	@echo -e "$(CYAN)Running profiling tests...$(NC)"
	$(GO) test -race -v -timeout $(TEST_TIMEOUT) ./profiling/...
	@echo -e "$(GREEN)✓ Profiling tests passed$(NC)"

# ============================================================================
# COMPREHENSIVE SUITES
# ============================================================================

.PHONY: test-all
test-all: dirs ## Run the full test suite with race detector and coverage
	@echo -e "$(MAGENTA)Running full test suite...$(NC)"
	@$(MAKE) test-race
	@$(MAKE) test-coverage
	@echo -e "$(GREEN)✓ Full test suite complete$(NC)"

.PHONY: test-ci
test-ci: dirs ## Run the CI test suite (race + coverage threshold check)
	@echo -e "$(MAGENTA)Running CI test suite...$(NC)"
	@$(MAKE) vet
	@$(MAKE) lint
	@$(MAKE) test-race
	@$(MAKE) test-coverage-check
	@echo -e "$(GREEN)✓ CI suite passed$(NC)"

.PHONY: test-smoke
test-smoke: dirs ## Quick smoke test — open, write, flush, restore
	@echo -e "$(CYAN)Running smoke tests...$(NC)"
	$(GO) test -v -timeout 30s -run "TestOpenClose|TestExecQuery|TestFlushAndRestore" .
	@echo -e "$(GREEN)✓ Smoke tests passed$(NC)"

# ============================================================================
# BENCHMARKS
# ============================================================================

.PHONY: bench
bench: dirs ## Run all benchmarks (5s per bench, memory stats)
	@echo -e "$(BLUE)Running benchmarks...$(NC)"
	$(GO) test -bench=. -benchmem -benchtime=5s -run=^$$ ./... | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Benchmark results saved to: $(BENCH_FILE)$(NC)"

# ============================================================================
# PROFILING
# ============================================================================
#
# The pprof-capturing tests live in memdb_pprof_test.go and are gated behind
# the MEMDB_PPROF environment variable so the default `go test ./...` run does
# not write profile artefacts. Output files are written to MEMDB_PPROF_DIR if
# set, otherwise to a per-test t.TempDir. After a run, analyse the resulting
# profiles with `go tool pprof -http=: <file>`.

.PHONY: pprof
pprof: dirs ## Run all pprof-capturing tests (writes CPU/heap/mutex/block profiles to PPROF_DIR)
	@mkdir -p $(COVERAGE_DIR)/pprof
	@echo -e "$(BLUE)Capturing pprof profiles into $(COVERAGE_DIR)/pprof...$(NC)"
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 300s -run "TestPProf_" .
	@echo -e "$(GREEN)✓ Profiles written to $(COVERAGE_DIR)/pprof/$(NC)"
	@ls -1 $(COVERAGE_DIR)/pprof/ 2>/dev/null || true

.PHONY: pprof-writes
pprof-writes: dirs ## Capture CPU/heap profiles of a pure-write workload
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Writes$$" .
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_writes.*$(NC)"

.PHONY: pprof-reads
pprof-reads: dirs ## Capture CPU/mutex/block profiles of a concurrent read workload (replica pool)
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Reads_Replicas$$" .
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_reads.*$(NC)"

.PHONY: pprof-mixed
pprof-mixed: dirs ## Capture CPU/heap profiles of a mixed read/write workload
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_MixedReadWrite$$" .
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_mixed.*$(NC)"

.PHONY: pprof-flush
pprof-flush: dirs ## Capture CPU/heap profiles of the flush path at 50k rows
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 120s -run "TestPProf_Flush$$" .
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_flush.*$(NC)"

.PHONY: pprof-wal
pprof-wal: dirs ## Capture CPU/heap profiles of DurabilityWAL writes
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Writes_WAL$$" .
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_writes_wal.*$(NC)"

.PHONY: pprof-server
pprof-server: dirs ## Capture CPU/heap/mutex/block profiles of the PostgreSQL wire-protocol server under sustained client load
	@mkdir -p $(COVERAGE_DIR)/pprof
	@echo -e "$(BLUE)Capturing server pprof profiles into $(COVERAGE_DIR)/pprof...$(NC)"
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 120s -run "TestPProf_Server_" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_*$(NC)"

.PHONY: pprof-server-select
pprof-server-select: dirs ## Narrow SELECT workload (~10 rows/query, 16 clients)
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Server_Select$$" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_select.*$(NC)"

.PHONY: pprof-server-select-wide
pprof-server-select-wide: dirs ## Wide SELECT workload (500 rows/query, amplifies per-row allocation cost)
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Server_Select_Wide$$" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_select_wide.*$(NC)"

.PHONY: pprof-server-insert
pprof-server-insert: dirs ## Concurrent INSERT DML through the simple-query protocol
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Server_Insert$$" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_insert.*$(NC)"

.PHONY: pprof-server-mixed
pprof-server-mixed: dirs ## Mixed SELECT/INSERT with mutex and block sampling enabled
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Server_Mixed$$" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_mixed.*$(NC)"

.PHONY: pprof-server-connect
pprof-server-connect: dirs ## Rapid connect/query/disconnect cycle (short-lived-connection overhead)
	@mkdir -p $(COVERAGE_DIR)/pprof
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 60s -run "TestPProf_Server_Connect$$" ./server/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_server_connect.*$(NC)"

.PHONY: pprof-raft
pprof-raft: dirs ## Capture CPU/heap profile of the Raft Apply path (3-node cluster, sustained writes through consensus)
	@mkdir -p $(COVERAGE_DIR)/pprof
	@echo -e "$(BLUE)Capturing Raft pprof profiles into $(COVERAGE_DIR)/pprof...$(NC)"
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 120s -run "TestPProf_Raft_" ./replication/raft/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_raft_*$(NC)"

.PHONY: pprof-raft-memdb
pprof-raft-memdb: dirs ## Capture CPU/heap profile of the full Raft+memdb apply path (3-node cluster, real *memdb.DB FSMs)
	@mkdir -p $(COVERAGE_DIR)/pprof
	@echo -e "$(BLUE)Capturing Raft+memdb pprof profiles into $(COVERAGE_DIR)/pprof...$(NC)"
	MEMDB_PPROF=1 MEMDB_PPROF_DIR=$(COVERAGE_DIR)/pprof \
		$(GO) test -v -timeout 120s -run "TestPProf_Raft_MemDB$$" ./replication/raft/...
	@echo -e "$(GREEN)✓ Profiles: $(COVERAGE_DIR)/pprof/pprof_raft_memdb.*$(NC)"

.PHONY: pprof-view
pprof-view: ## Open the last captured CPU profile in the pprof web UI (PROF=<path>)
	@if [ -z "$(PROF)" ]; then \
		echo -e "$(YELLOW)PROF not set — defaulting to $(COVERAGE_DIR)/pprof/pprof_writes.cpu.prof$(NC)"; \
		$(GO) tool pprof -http=: $(COVERAGE_DIR)/pprof/pprof_writes.cpu.prof; \
	else \
		$(GO) tool pprof -http=: $(PROF); \
	fi

.PHONY: bench-core
bench-core: dirs ## Run core memdb benchmarks only (Exec, Query, Flush, WAL, lifecycle)
	@echo -e "$(BLUE)Running core benchmarks...$(NC)"
	$(GO) test -bench='^BenchmarkExec|^BenchmarkQuery|^BenchmarkFlush|^BenchmarkWAL|^BenchmarkOpen' \
		-benchmem -benchtime=5s -run=^$$ . | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Core benchmark results: $(BENCH_FILE)$(NC)"

.PHONY: bench-compare
bench-compare: dirs ## Run memdb vs file SQLite comparison benchmarks
	@echo -e "$(BLUE)Running comparison benchmarks (memdb vs file SQLite)...$(NC)"
	$(GO) test -bench='^BenchmarkCompare' -benchmem -benchtime=5s -run=^$$ . | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Comparison results: $(BENCH_FILE)$(NC)"

.PHONY: bench-concurrency
bench-concurrency: dirs ## Run concurrent read/write benchmarks across goroutine counts
	@echo -e "$(BLUE)Running concurrency benchmarks (-cpu=1,4,8)...$(NC)"
	$(GO) test -bench='^BenchmarkConcurrentReadWrite|^BenchmarkCompare_ConcurrentRead' \
		-benchmem -benchtime=5s -cpu=1,4,8 -run=^$$ . | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Concurrency benchmark results: $(BENCH_FILE)$(NC)"

.PHONY: bench-wal
bench-wal: dirs ## Run WAL append and replay benchmarks
	@echo -e "$(BLUE)Running WAL benchmarks...$(NC)"
	$(GO) test -bench='^BenchmarkWAL' -benchmem -benchtime=5s -run=^$$ . | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ WAL benchmark results: $(BENCH_FILE)$(NC)"

.PHONY: bench-flush
bench-flush: dirs ## Run flush benchmarks at varying table sizes
	@echo -e "$(BLUE)Running flush benchmarks...$(NC)"
	$(GO) test -bench='^BenchmarkFlush' -benchmem -benchtime=5s -run=^$$ . | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Flush benchmark results: $(BENCH_FILE)$(NC)"

.PHONY: bench-stat
bench-stat: bench  ## Run benchmarks then summarise with benchstat (requires: go install golang.org/x/perf/cmd/benchstat@latest)
	@echo -e "$(BLUE)Analysing benchmark results...$(NC)"
	@if command -v benchstat >/dev/null 2>&1; then \
		benchstat $(BENCH_FILE); \
	else \
		echo -e "$(YELLOW)benchstat not found. Run: go install golang.org/x/perf/cmd/benchstat@latest$(NC)"; \
		cat $(BENCH_FILE); \
	fi

.PHONY: bench-pprof
bench-pprof: dirs ## Run all benchmarks with CPU+mem profile capture (writes to $(COVERAGE_DIR)/pprof)
	@mkdir -p $(COVERAGE_DIR)/pprof
	@echo -e "$(BLUE)Running benchmarks with pprof capture...$(NC)"
	$(GO) test -bench=. -benchmem -benchtime=3s -run=^$$ \
		-cpuprofile=$(COVERAGE_DIR)/pprof/bench.cpu.prof \
		-memprofile=$(COVERAGE_DIR)/pprof/bench.mem.prof \
		-mutexprofile=$(COVERAGE_DIR)/pprof/bench.mutex.prof \
		-blockprofile=$(COVERAGE_DIR)/pprof/bench.block.prof \
		. | tee $(BENCH_FILE)
	@echo -e "$(GREEN)✓ Benchmark + pprof results in $(COVERAGE_DIR)/pprof/$(NC)"

# ============================================================================
# CODE QUALITY
# ============================================================================

.PHONY: fmt
fmt: ## Format all Go source files
	@echo -e "$(BLUE)Formatting code...$(NC)"
	$(GO) fmt ./...
	@echo -e "$(GREEN)✓ Code formatted$(NC)"

.PHONY: fmt-check
fmt-check: ## Check formatting without modifying files (CI-safe)
	@echo -e "$(BLUE)Checking code formatting...$(NC)"
	@UNFORMATTED=$$(gofmt -l .); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo -e "$(RED)✗ Unformatted files:$(NC)"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi
	@echo -e "$(GREEN)✓ All files correctly formatted$(NC)"

.PHONY: vet
vet: ## Run go vet across all packages
	@echo -e "$(BLUE)Running go vet...$(NC)"
	$(GO) vet ./...
	@echo -e "$(GREEN)✓ go vet clean$(NC)"

.PHONY: lint
lint: ## Run golangci-lint
	@echo -e "$(BLUE)Running golangci-lint...$(NC)"
	@if [ ! -f "$(GOLANGCI_LINT)" ] && ! command -v golangci-lint >/dev/null 2>&1; then \
		echo -e "$(YELLOW)golangci-lint not found. Run: make tools$(NC)"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) run ./...
	@echo -e "$(GREEN)✓ Lint clean$(NC)"

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with auto-fix enabled
	@echo -e "$(BLUE)Running golangci-lint with fixes...$(NC)"
	$(GOLANGCI_LINT) run --fix ./...
	@echo -e "$(GREEN)✓ Lint fixes applied$(NC)"

.PHONY: security
security: ## Run gosec security scanner
	@echo -e "$(BLUE)Running security scan...$(NC)"
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
		echo -e "$(GREEN)✓ Security scan complete$(NC)"; \
	else \
		echo -e "$(YELLOW)gosec not found. Run: make tools$(NC)"; \
	fi

.PHONY: check
check: fmt-check vet lint ## Run all static checks (fmt, vet, lint)
	@echo -e "$(GREEN)✓ All checks passed$(NC)"

# ============================================================================
# INSTALLATION
# ============================================================================

.PHONY: install
install: build ## Install the memdb binary to INSTALL_DIR (/usr/local/bin)
	@echo -e "$(BLUE)Installing $(BINARY_NAME) to $(INSTALL_DIR)...$(NC)"
	install -m 0755 $(BUILD_DIR)/$(BINARY_NAME) $(INSTALL_DIR)/$(BINARY_NAME)
	@echo -e "$(GREEN)✓ Installed: $(INSTALL_DIR)/$(BINARY_NAME)$(NC)"

.PHONY: uninstall
uninstall: ## Remove the installed memdb binary
	@echo -e "$(BLUE)Removing $(INSTALL_DIR)/$(BINARY_NAME)...$(NC)"
	rm -f $(INSTALL_DIR)/$(BINARY_NAME)
	@echo -e "$(GREEN)✓ Uninstalled$(NC)"

# ============================================================================
# RELEASE
# ============================================================================

.PHONY: release
release: clean check test build-all ## Full release: clean → check → test → build all platforms
	@echo -e "$(BLUE)Creating release packages for $(VERSION)...$(NC)"
	@mkdir -p $(BUILD_DIR)/releases
	@for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do \
		echo -e "  Packaging $$platform..."; \
		tar -czf $(BUILD_DIR)/releases/$(BINARY_NAME)-$(VERSION)-$$platform.tar.gz \
			-C $(BUILD_DIR)/$$platform $(BINARY_NAME); \
		echo -e "$(GREEN)  ✓ $(BUILD_DIR)/releases/$(BINARY_NAME)-$(VERSION)-$$platform.tar.gz$(NC)"; \
	done
	@echo -e "$(GREEN)✓ Release $(VERSION) complete$(NC)"

.PHONY: changelog
changelog: ## Generate CHANGELOG.md from git log since last tag
	@echo -e "$(BLUE)Generating changelog...$(NC)"
	@LAST_TAG=$$(git describe --tags --abbrev=0 2>/dev/null || echo ""); \
	if [ -n "$$LAST_TAG" ]; then \
		git log --pretty=format:"* %s (%h)" $$LAST_TAG..HEAD > CHANGELOG.md; \
	else \
		git log --pretty=format:"* %s (%h)" > CHANGELOG.md; \
	fi
	@echo -e "$(GREEN)✓ CHANGELOG.md generated$(NC)"

.PHONY: tag
tag: ## Create and push a git tag — usage: make tag VERSION=v1.2.0
	@if [ -z "$(VERSION_TAG)" ]; then \
		echo -e "$(RED)Error: VERSION_TAG not set. Usage: make tag VERSION_TAG=v1.2.0$(NC)"; \
		exit 1; \
	fi
	git tag -a $(VERSION_TAG) -m "$(VERSION_TAG)"
	git push origin $(VERSION_TAG)
	@echo -e "$(GREEN)✓ Tagged and pushed $(VERSION_TAG)$(NC)"

# ============================================================================
# CLEANUP
# ============================================================================

.PHONY: clean
clean: ## Remove build artifacts and coverage reports
	@echo -e "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -rf $(COVERAGE_DIR)
	@rm -f coverage.out coverage.html
	@echo -e "$(GREEN)✓ Clean$(NC)"

.PHONY: clean-tests
clean-tests: ## Remove test cache and coverage reports
	@echo -e "$(BLUE)Cleaning test artifacts...$(NC)"
	@rm -rf $(COVERAGE_DIR)
	@rm -f coverage.out coverage.html
	$(GO) clean -testcache
	@echo -e "$(GREEN)✓ Test artifacts cleaned$(NC)"

.PHONY: clean-all
clean-all: clean ## Deep clean including module and build caches
	@echo -e "$(BLUE)Deep cleaning...$(NC)"
	$(GO) clean -cache -testcache -modcache
	@echo -e "$(GREEN)✓ Deep clean complete$(NC)"

# ============================================================================
# HELP
# ============================================================================

.PHONY: help
help: ## Show this help message
	@echo -e "$(BLUE)memdb — in-memory SQLite with persistence, replication, and Postgres wire protocol$(NC)"
	@echo -e "$(YELLOW)Version: $(VERSION)  Commit: $(GIT_COMMIT)$(NC)"
	@echo ""
	@echo -e "$(YELLOW)Usage:$(NC)"
	@echo "  make <target>"
	@echo ""
	@echo -e "$(YELLOW)Build:$(NC)"
	@echo "  build              Build the memdb CLI binary"
	@echo "  build-prod         Build optimised production binary (stripped)"
	@echo "  build-all          Build for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64"
	@echo "  run                Build and run the server with default options"
	@echo "  run-args           Run with custom args (ARGS=\"serve --file /tmp/x.db\")"
	@echo ""
	@echo -e "$(YELLOW)Core Testing:$(NC)"
	@echo "  test               Run all tests"
	@echo "  test-race          Run all tests with the race detector"
	@echo "  test-short         Run short tests only"
	@echo "  test-coverage      Run all tests and generate HTML coverage report"
	@echo "  test-coverage-check  Fail if coverage is below $(COVERAGE_THRESHOLD)%"
	@echo "  test-specific      Run a specific test  (TEST=TestName)"
	@echo "  test-count         Print count of passing tests"
	@echo ""
	@echo -e "$(CYAN)Package Tests:$(NC)"
	@echo "  test-core          Root memdb package"
	@echo "  test-backends      Compressed / Encrypted / Local backends"
	@echo "  test-server        PostgreSQL wire-protocol server"
	@echo "  test-replication   Leader / Follower WAL shipping"
	@echo "  test-raft          Raft consensus FSM"
	@echo "  test-wal           WAL append, replay, truncate"
	@echo "  test-durability    DurabilityNone / WAL / Sync modes"
	@echo "  test-replica       Read replica pool"
	@echo ""
	@echo -e "$(MAGENTA)Suites:$(NC)"
	@echo "  test-all           Race detector + coverage report"
	@echo "  test-ci            vet + lint + race + coverage threshold"
	@echo "  test-smoke         Open / write / flush / restore only"
	@echo ""
	@echo -e "$(YELLOW)Benchmarks:$(NC)"
	@echo "  bench              All benchmarks (5s each)"
	@echo "  bench-core         Exec / Query / Flush / WAL / lifecycle"
	@echo "  bench-compare      memdb vs file SQLite across all operations"
	@echo "  bench-concurrency  Concurrent read/write at -cpu=1,4,8"
	@echo "  bench-wal          WAL append and replay"
	@echo "  bench-flush        Flush at 100 / 1 000 / 10 000 rows"
	@echo "  bench-stat         Run benchmarks then analyse with benchstat"
	@echo "  bench-pprof        Benchmarks with CPU/mem/mutex/block profile capture"
	@echo ""
	@echo -e "$(MAGENTA)Profiling (pprof — core memdb):$(NC)"
	@echo "  pprof              Run all pprof-capturing tests"
	@echo "  pprof-writes       Pure-write workload profile"
	@echo "  pprof-reads        Concurrent read workload profile (replica pool)"
	@echo "  pprof-mixed        Mixed read/write workload profile"
	@echo "  pprof-flush        Flush path profile at 50k rows"
	@echo "  pprof-wal          DurabilityWAL writes profile"
	@echo "  pprof-view         Open a profile in the pprof web UI (PROF=<path>)"
	@echo ""
	@echo -e "$(MAGENTA)Profiling (pprof — Postgres wire server):$(NC)"
	@echo "  pprof-server             Run all server pprof scenarios"
	@echo "  pprof-server-select      Narrow SELECT (~10 rows/query, 16 clients)"
	@echo "  pprof-server-select-wide Wide SELECT (500 rows/query, 8 clients)"
	@echo "  pprof-server-insert      Concurrent INSERT DML"
	@echo "  pprof-server-mixed       Mixed SELECT/INSERT + mutex/block sampling"
	@echo "  pprof-server-connect     Rapid connect/query/disconnect cycles"
	@echo ""
	@echo -e "$(CYAN)Replication & Profiling Tests:$(NC)"
	@echo "  test-replication-integrity  3-node end-to-end Raft integrity tests"
	@echo "  test-replication-soak       High-volume soak + snapshot integrity"
	@echo "  test-profiling              profiling package unit tests"
	@echo ""
	@echo -e "$(YELLOW)Code Quality:$(NC)"
	@echo "  fmt                Format all source files"
	@echo "  fmt-check          Check formatting without modifying (CI-safe)"
	@echo "  vet                Run go vet"
	@echo "  lint               Run golangci-lint"
	@echo "  lint-fix           Run golangci-lint with auto-fix"
	@echo "  security           Run gosec security scanner"
	@echo "  check              fmt-check + vet + lint"
	@echo ""
	@echo -e "$(YELLOW)Dependencies:$(NC)"
	@echo "  deps               Download and tidy module dependencies"
	@echo "  deps-upgrade       Upgrade all dependencies to latest"
	@echo "  tools              Install golangci-lint, benchstat, gosec"
	@echo ""
	@echo -e "$(YELLOW)Install:$(NC)"
	@echo "  install            Install binary to $(INSTALL_DIR)"
	@echo "  uninstall          Remove binary from $(INSTALL_DIR)"
	@echo ""
	@echo -e "$(YELLOW)Release:$(NC)"
	@echo "  release            clean → check → test → build-all → package tarballs"
	@echo "  changelog          Generate CHANGELOG.md from git log"
	@echo "  tag                Create and push a git tag  (VERSION_TAG=v1.x.y)"
	@echo ""
	@echo -e "$(YELLOW)Cleanup:$(NC)"
	@echo "  clean              Remove build and coverage directories"
	@echo "  clean-tests        Remove test cache and coverage reports"
	@echo "  clean-all          Deep clean including module and build caches"
	@echo ""
	@echo -e "$(YELLOW)Environment Variables:$(NC)"
	@echo "  GOOS               Target OS  (linux, darwin)"
	@echo "  GOARCH             Target arch (amd64, arm64)"
	@echo "  TEST               Test name for test-specific target"
	@echo "  ARGS               Arguments for run-args target"
	@echo "  VERSION_TAG        Tag name for tag target  (e.g. v1.2.0)"
	@echo "  COVERAGE_THRESHOLD Minimum coverage % for test-coverage-check (default: $(COVERAGE_THRESHOLD))"
