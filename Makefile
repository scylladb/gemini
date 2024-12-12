MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

GO111MODULE := on
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)

DOCKER_COMPOSE_TESTING ?= scylla
DOCKER_VERSION ?= latest
GOLANGCI_VERSION ?= 1.62.0

CQL_FEATURES ?= normal
CONCURRENCY ?= 1
DURATION ?= 10m
WARMUP ?= 0
MODE ?= mixed
DATASET_SIZE ?= large
SEED ?= $(shell date +%s)
GEMINI_BINARY ?= $(PWD)/bin/gemini
GEMINI_TEST_CLUSTER ?= $(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test)
GEMINI_ORACLE_CLUSTER ?= $(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle)
GEMINI_DOCKER_NETWORK ?= gemini
GEMINI_FLAGS ?= --fail-fast \
	--async-objects-stabilization-attempts=10 \
	--async-objects-stabilization-backoff=1s \
	--concurrency=$(CONCURRENCY) \
 	--cql-features=$(CQL_FEATURES) \
 	--dataset-size=$(DATASET_SIZE) \
 	--mode=$(MODE) \
 	--duration=$(DURATION) \
	--seed=$(SEED) \
	--schema-seed=$(SEED) \
	--warmup=$(WARMUP) \
 	--drop-schema=true \
 	--level=info \
 	--materialized-views=false \
	--async-objects-stabilization-attempts=10 \
	--profiling-port=6060 \
	--token-range-slices=10000 \
	--partition-key-buffer-reuse-size=100 \
	--oracle-connect-timeout=15s \
	--oracle-request-timeout=5s \
	--oracle-consistency=LOCAL_QUORUM \
	--oracle-max-mutation-retries=10 \
	--oracle-max-mutation-retries-backoff=1s \
	--oracle-use-server-timestamps=false \
	--oracle-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--oracle-host-selection-policy=token-aware \
	--test-consistency=LOCAL_QUORUM \
	--test-request-timeout=5s \
	--test-connect-timeout=15s \
	--test-use-server-timestamps=false \
	--test-host-selection-policy=token-aware \
	--test-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--test-max-mutation-retries=10 \
	--test-dc=datacenter1

ifndef GOBIN
	export GOBIN := $(MAKEFILE_PATH)/bin
endif

define dl_tgz
	@mkdir -p $(GOBIN) 2>/dev/null

	@if [ ! -f "$(GOBIN)/$(1)" ]; then \
		echo "Downloading $(GOBIN)/$(1)"; \
		curl --progress-bar -L $(2) | tar zxf - --wildcards --strip 1 -C $(GOBIN) '*/$(1)'; \
		chmod +x "$(GOBIN)/$(1)"; \
	fi
endef

$(GOBIN)/golangci-lint:
$(GOBIN)/golangci-lint: Makefile
	$(call dl_tgz,golangci-lint,https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-amd64.tar.gz)

.PHONY: fmt
fmt:
	@gofumpt -w -extra .

.PHONY: check
check: $(GOBIN)/golangci-lint
	@$(GOBIN)/golangci-lint run

.PHONY: fix
fix: $(GOBIN)/golangci-lint
	@$(GOBIN)/golangci-lint run --fix

.PHONY: build
build:
	@CGO_ENABLED=0 go build -ldflags="-s -w"  -o bin/gemini ./cmd/gemini

.PHONY: debug-build
debug-build:
	@CGO_ENABLED=0 go build -gcflags="-N -l" -o bin/gemini ./cmd/gemini

.PHONY: build-docker
build-docker:
	@docker build --target production -t scylladb/gemini:$(DOCKER_VERSION) --compress .

.PHONY: setup
setup: $(GOBIN)/golangci-lint scylla-setup debug-build
	@pre-commit install

.PHONY: scylla-setup
scylla-setup:
	@docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml up -d

	@until docker logs gemini-oracle 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	@until docker logs gemini-test 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done

.PHONY: scylla-shutdown
scylla-shutdown:
	@docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml down --volumes

.PHONY: test
test:
	@go test -covermode=atomic -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | gotestfmt -showteststatus

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
	@$(GEMINI_BINARY) \
		--test-cluster=$(GEMINI_TEST_CLUSTER) \
		--oracle-cluster=$(GEMINI_ORACLE_CLUSTER) \
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
