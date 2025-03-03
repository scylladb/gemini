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
GEMINI_TEST_CLUSTER ?=
GEMINI_DOCKER_NETWORK ?= gemini
GEMINI_FLAGS ?= --fail-fast \
	--concurrency=$(CONCURRENCY) \
	--mode=$(MODE) \
	--seed=$(SEED) \
	--schema-seed=$(SEED) \
	--level=info \
	--async-objects-stabilization-attempts=10 \
	--dataset-size=$(DATASET_SIZE) \
	--cql-features=$(CQL_FEATURES) \
	--duration=$(DURATION) \
	--warmup=$(WARMUP) \
	--profiling-port=6060 \
	--drop-schema=true \
	--token-range-slices=10000 \
	--partition-key-distribution=uniform \
	--partition-key-buffer-reuse-size=100 \
	--max-errors-to-store=1000 \
	--test-consistency=LOCAL_QUORUM \
	--test-dc=datacenter1 \
	--test-host-selection-policy=token-aware \
	--test-statement-log-file=./results/gemini.log \
	--test-max-mutation-retries=10 \
	--test-request-timeout=5s \
	--test-connect-timeout=15s \
	--test-use-server-timestamps=false \
	--test-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--oracle-host-selection-policy=token-aware \
	--oracle-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--oracle-consistency=LOCAL_QUORUM \
	--oracle-dc=datacenter1 \
	--oracle-statement-log-file=./results/gemini_oracle.log \
	--oracle-request-timeout=5s \
	--oracle-connect-timeout=15s \
	--oracle-use-server-timestamps=false \
	--oracle-max-mutation-retries=10

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

define run_integration_tests

endef

$(GOBIN)/golangci-lint:
$(GOBIN)/golangci-lint: Makefile
	$(call dl_tgz,golangci-lint,https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-amd64.tar.gz)

.PHONY: integration-test
integration-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	@$(GEMINI_BINARY) \
		--test-cluster=$(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test) \
		--oracle-cluster=$(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle) \
		$(GEMINI_FLAGS)

SCYLLA_TEST_1 := $(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test-1)
SCYLLA_TEST_2 := $(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test-2)
SCYLLA_TEST_3 := $(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test-3)

.PHONY: integration-cluster-test
integration-cluster-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	$(GEMINI_BINARY) \
		--test-cluster="$(SCYLLA_TEST_1),$(SCYLLA_TEST_2),$(SCYLLA_TEST_3)" \
		--oracle-cluster="$(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle)" \
		$(GEMINI_FLAGS)

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
	@go test -covermode=atomic -tags testing -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | gotestfmt -showteststatus

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


.PHONY: clean
clean: clean-bin clean-results

.PHONY: clean-bin
clean-bin:
	@rm -rf bin

.PHONY: clean-results
clean-results:
	@rm -rf results/*.log
	@rm -rf results/gemini_seed
