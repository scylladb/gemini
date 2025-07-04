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
CONCURRENCY ?= 2
DURATION ?= 10m
WARMUP ?= 30s
MODE ?= mixed
DATASET_SIZE ?= large
GEMINI_SEED := $(shell echo $$((RANDOM % 100 + 1)))
GEMINI_BINARY ?= $(PWD)/bin/gemini
GEMINI_DOCKER_NETWORK ?= gemini
GEMINI_IO_WORKER_POOL ?= 1024

define get_scylla_ip
	$(shell docker inspect --format "{{ .NetworkSettings.Networks.$(GEMINI_DOCKER_NETWORK).IPAddress }}" "$(1)")
endef

GEMINI_FLAGS ?= --level=debug \
	--consistency=QUORUM \
	--test-host-selection-policy=token-aware \
	--oracle-host-selection-policy=token-aware \
	--mode=$(MODE) \
	--async-objects-stabilization-attempts=10 \
	--oracle-replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
	--concurrency=$(CONCURRENCY) \
	--dataset-size=$(DATASET_SIZE) \
	--seed=$(GEMINI_SEED) \
	--schema-seed=$(GEMINI_SEED) \
	--cql-features=$(CQL_FEATURES) \
	--duration=$(DURATION) \
	--warmup=$(WARMUP) \
	--partition-key-buffer-reuse-size=50 \
	--partition-key-distribution=uniform \
	--io-worker-pool=$(GEMINI_IO_WORKER_POOL) \
	--oracle-statement-log-file=$(PWD)/results/oracle-statements.json \
	--test-statement-log-file=$(PWD)/results/test-statements.json

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
	@go tool fieldalignment -fix ./pkg/...

.PHONY: fmt
fmt:
	@go tool golangci-lint fmt

.PHONY: build
build:
	@mkdir -p bin
	@go build \
		-a -installsuffix cgo \
		-trimpath \
		-tags="production,!debug,netgo,osusergo,static_build" \
		-gcflags="-wb=false -l=4 -B -C -m -m -live -d=ssa/check/on" \
		-ldflags="-linkmode=external -extldflags '-static' -s -w $(LDFLAGS_VERSION)" \
		-o bin/gemini ./pkg/cmd
	@./bin/gemini --version --version-json > bin/version.json

.PHONY: debug-build
debug-build:
	@mkdir -p bin
	@go build -ldflags="$(LDFLAGS_VERSION)" -gcflags="-N -l" -o bin/gemini ./pkg/cmd
	@./bin/gemini --version --version-json > bin/version.json

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

.PHONY: stop-scylla-monitoring
stop-scylla-monitoring:
	cd scylla-monitoring && ./kill-all.sh

SCYLLA_MONITORING_VERSION ?= 4.10.0
SCYLLA_TEST_VERSION ?= 2025.1
SCYLLA_ORACLE_VERSION ?= 2025.1

.PHONY: scylla-monitoring
scylla-monitoring:
	@if [ ! -d "scylla-monitoring" ]; then \
		wget -c "https://github.com/scylladb/scylla-monitoring/archive/refs/tags/$(SCYLLA_MONITORING_VERSION).zip"; \
		unzip $(SCYLLA_MONITORING_VERSION).zip; \
		mv scylla-monitoring-$(SCYLLA_MONITORING_VERSION) scylla-monitoring; \
		rm -f $(SCYLLA_MONITORING_VERSION).zip; \
		cp docker/monitoring/scylla_servers.yml scylla-monitoring/prometheus/scylla_servers.yml; \
		mkdir -p scylla-monitoring/grafana/ver_$(SCYLLA_TEST_VERSION); \
		cp docker/monitoring/Gemini.json scylla-monitoring/grafana/build/ver_$(SCYLLA_TEST_VERSION)/gemini.json; \
	fi;

	cd scylla-monitoring \
		&& ./start-all.sh -v 2025.1 -l \
		--no-loki \
		--no-alertmanager \
		--auto-restart \
		--enable-protobuf

.PHONY: scylla-setup-cluster
scylla-setup-cluster: scylla-monitoring
	SCYLLA_ORACLE_VERSION=$(SCYLLA_ORACLE_VERSION) SCYLLA_TEST_VERSION=$(SCYLLA_TEST_VERSION) docker compose -f docker/docker-compose-scylla-cluster.yml up -d --wait

	until docker logs gemini-oracle 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-1 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-2 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test-3 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	$(MAKE) scylla-monitoring

.PHONY: scylla-shutdown-cluster
scylla-shutdown-cluster: stop-scylla-monitoring
	@docker compose -f docker/docker-compose-scylla-cluster.yml down --volumes

.PHONY: test
test:
	@go test -covermode=atomic -gcflags="-N -l" -tags testing -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | go tool gotestfmt -showteststatus

PPROF_PORT ?= 6060
PPROF_SECONDS ?= 60
.PHONY: pprof-profile
pprof-profile:
	@go tool pprof -http=:8080 'http://localhost:$(PPROF_PORT)/debug/pprof/profile?seconds=$(PPROF_SECONDS)'

.PHONY: pprof-heap
pprof-heap:
	@go tool pprof -http=:8085 "http://localhost:$(PPROF_PORT)/debug/pprof/heap?seconds=$(PPROF_SECONDS)"

.PHONY: pprof-goroutine
pprof-goroutine:
	@go tool pprof -http=:8082 "http://localhost:$(PPROF_PORT)/debug/pprof/goroutine?debug=1"

.PHONY: pprof-block
pprof-block:
	@go tool pprof -http=:8083 'http://localhost:$(PPROF_PORT)/debug/pprof/block'

.PHONY: pprof-mutex
pprof-mutex:
	@go tool pprof -http=:8084 http://localhost:$(PPROF_PORT)/debug/pprof/mutex

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
			--replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
			$(GEMINI_FLAGS)

.PHONY: integration-test
integration-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	GODEBUG="default=go1.24,cgocheck=1,disablethp=0,panicnil=0,http2client=1,http2server=1,asynctimerchan=0,madvdontneed=0" GOGC="95" $(GEMINI_BINARY) \
		--test-cluster="$(call get_scylla_ip,gemini-test)" \
		--oracle-cluster="$(call get_scylla_ip,gemini-oracle)" \
		--replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
		$(GEMINI_FLAGS)

.PHONY: integration-cluster-test
integration-cluster-test:
	@mkdir -p $(PWD)/results
	@touch $(PWD)/results/gemini_seed
	@echo $(GEMINI_SEED) > $(PWD)/results/gemini_seed
	GODEBUG="default=go1.24,cgocheck=1,disablethp=0,panicnil=0,http2client=1,http2server=1,asynctimerchan=0,madvdontneed=0" GOGC="95" \
	$(GEMINI_BINARY) \
		--test-cluster="$(call get_scylla_ip,gemini-test-1),$(call get_scylla_ip,gemini-test-2),$(call get_scylla_ip,gemini-test-3)" \
		--oracle-cluster="$(call get_scylla_ip,gemini-oracle)" \
		--replication-strategy="{'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}" \
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
