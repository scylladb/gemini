MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

GO111MODULE := on
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)

DOCKER_COMPOSE_TESTING ?= scylla
DOCKER_VERSION ?= latest

VERSION ?= $(shell git describe --tags 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell git log -1 --format=%cd --date=format:%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS_VERSION := -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(BUILD_DATE)

CQL_FEATURES ?= normal
CONCURRENCY ?= 16
DURATION ?= 10m
WARMUP ?= 2m
MODE ?= mixed
DATASET_SIZE ?= large
GEMINI_SEED := $(shell echo $$((RANDOM % 100 + 1)))
GEMINI_BINARY ?= $(PWD)/bin/gemini
GEMINI_DOCKER_NETWORK ?= gemini

define get_scylla_ip
	$(shell docker inspect --format "{{ .NetworkSettings.Networks.$(GEMINI_DOCKER_NETWORK).IPAddress }}" "$(1)")
endef

GEMINI_FLAGS ?= --fail-fast \
	--level=info \
	--consistency=QUORUM \
	--test-host-selection-policy=token-aware \
	--oracle-host-selection-policy=token-aware \
	--mode=$(MODE) \
	--request-timeout=5s \
	--connect-timeout=15s \
	--use-server-timestamps=true \
	--async-objects-stabilization-attempts=10 \
	--max-mutation-retries=10 \
	--replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--oracle-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--concurrency=$(CONCURRENCY) \
	--dataset-size=$(DATASET_SIZE) \
	--seed=$(GEMINI_SEED) \
	--schema-seed=$(GEMINI_SEED) \
	--cql-features=$(CQL_FEATURES) \
	--duration=$(DURATION) \
	--warmup=$(WARMUP) \
	--profiling-port=6060 \
	--drop-schema=true \
	--token-range-slices=10000 \
	--partition-key-buffer-reuse-size=100 \
	--partition-key-distribution=uniform \
	--oracle-statement-log-file=$(PWD)/results/oracle-statements.log.zst \
	--test-statement-log-file=$(PWD)/results/test-statements.log.zst \
	--statement-log-file-compression=zstd

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: check
check:
	@go tool golangci-lint run

.PHONY: fix
fix:
	@go tool golangci-lint run --fix

.PHONY: fieldalign
fieldalign:
	@go tool fieldalignment -fix ./cmd/...
	@go tool fieldalignment -fix ./pkg/...

.PHONY: fmt
fmt:
	@go tool golangci-lint fmt

.PHONY: build
build:
	@CGO_ENABLED=0 go build -ldflags="-s -w"  -ldflags="$(LDFLAGS_VERSION)"  -o bin/gemini ./cmd/gemini

.PHONY: debug-build
debug-build:
	@CGO_ENABLED=0 go build -ldflags="$(LDFLAGS_VERSION)" -gcflags="-N -l" -o bin/gemini ./cmd/gemini

.PHONY: build-docker
build-docker:
	@docker build --target production -t scylladb/gemini:$(DOCKER_VERSION) --compress .

.PHONY: setup
setup: scylla-setup debug-build
	@pre-commit install

.PHONY: scylla-setup
scylla-setup:
	@docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml up -d --wait

	until docker logs gemini-oracle 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done


.PHONY: scylla-shutdown
scylla-shutdown:
	@docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml down --volumes

.PHONY: scylla-setup-cluster
scylla-setup-cluster:
	@docker compose -f docker/docker-compose-scylla-cluster.yml up -d --wait

	until docker logs gemini-oracle 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-1 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-2 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-3 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done

.PHONY: scylla-shutdown-cluster
scylla-shutdown-cluster:
	@docker compose -f docker/docker-compose-scylla-cluster.yml down --volumes

.PHONY: test
test:
	@go test -covermode=atomic -tags testing -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | go tool gotestfmt -showteststatus

.PHONY: pprof-profile
pprof-profile:
	@go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile

.PHONY: pprof-heap
pprof-heap:
	@go tool pprof -http=:8081 http://localhost:6060/debug/pprof/heap

.PHONY: pprof-goroutine
pprof-goroutine:
	@go tool pprof -http=:8082 http://localhost:6060/debug/pprof/goroutine

.PHONY: pprof-block
pprof-block:
	@go tool pprof -http=:8083 http://localhost:6060/debug/pprof/block

.PHONY: pprof-mutex
pprof-mutex:
	@go tool pprof -http=:8084 http://localhost:6060/debug/pprof/mutex

.PHONY: docker-integration-test
docker-integration-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	@docker run \
		-it \
		--rm \
		--memory=4G \
		-p 6060:6060 \
		--name gemini \
		--network $(GEMINI_DOCKER_NETWORK) \
		-v $(PWD)/results:/results \
		-w / \
		scylladb/gemini:$(DOCKER_VERSION) \
			--test-cluster=gemini-test \
			--oracle-cluster=gemini-oracle \
			$(GEMINI_FLAGS)

.PHONY: integration-test
integration-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	$(GEMINI_BINARY) \
		--test-cluster="$(call get_scylla_ip,gemini-test)" \
		--oracle-cluster="$(call get_scylla_ip,gemini-oracle)" \
		$(GEMINI_FLAGS)

.PHONY: integration-cluster-test
integration-cluster-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	$(GEMINI_BINARY) \
		--test-cluster="$(call get_scylla_ip,gemini-test-1),$(call get_scylla_ip,gemini-test-2),$(call get_scylla_ip,gemini-test-3)" \
		--oracle-cluster="$(call get_scylla_ip,gemini-oracle)" \
		$(GEMINI_FLAGS)

.PHONY: clean
clean: clean-bin clean-results

.PHONY: clean-bin
clean-bin:
	@rm -rf bin

.PHONY: clean-results
clean-results:
	@rm -rf results/*.log
	@rm -rf results/gemini_seed
