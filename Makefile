MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

GO111MODULE := on
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)

DOCKER_COMPOSE_TESTING := scylla
DOCKER_VERSION := latest

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

$(GOBIN)/golangci-lint: GOLANGCI_VERSION = 1.60.3
$(GOBIN)/golangci-lint: Makefile
	$(call dl_tgz,golangci-lint,https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-amd64.tar.gz)

.PHONY: check
check: $(GOBIN)/golangci-lint
	$(GOBIN)/golangci-lint run

.PHONY: fix
fix: $(GOBIN)/golangci-lint
	$(GOBIN)/golangci-lint run --fix

.PHONY: build
build:
	CGO_ENABLED=0 go build -ldflags="-asan" -gcflags "all=-N -l" -o bin/gemini ./cmd/gemini

debug-build:
	CGO_ENABLED=0 go build -gcflags "all=-N -l" -o bin/gemini ./cmd/gemini

.PHONY: build-docker
build-docker:
	docker build --target production -t scylladb/gemini:$(DOCKER_VERSION) --compress .

.PHONY: scylla-setup
scylla-setup:
	@docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml up -d

	until docker logs gemini-oracle 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done
	until docker logs gemini-test 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 0.2; done

.PHONY: scylla-shutdown
scylla-shutdown:
	docker compose -f docker/docker-compose-$(DOCKER_COMPOSE_TESTING).yml down --volumes

.PHONY: test
test:
	go test -covermode=atomic -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | gotestfmt -showteststatus

CQL_FEATURES := normal
CONCURRENCY := 16
DURATION := 10m
WARMUP := 1m
SEED := $(shell date +%s | tee ./results/gemini_seed)

.PHONY: integration-test
integration-test: build
	mkdir -p ./results
	touch ./results/gemini_seed
	./bin/gemini \
		--drop-schema true \
		--fail-fast \
		--non-interactive \
		--materialized-views false \
		--dataset-size=large \
		--outfile ./results/gemini.log \
		--test-statement-log-file ./results/gemini_test_statements.log \
		--oracle-statement-log-file ./results/gemini_oracle_statements.log \
		--test-host-selection-policy token-aware \
		--oracle-host-selection-policy token-aware \
		--seed=$(SEED) \
		--test-cluster=$(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-test) \
		--oracle-cluster=$(shell docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' gemini-oracle) \
		--outfile ./results/gemini_result.log \
		--duration $(DURATION) \
		--warmup $(WARMUP) \
		-m mixed \
		--non-interactive \
		--cql-features $(CQL_FEATURES) \
		--request-timeout 180s \
		--connect-timeout 120s \
		--consistency LOCAL_QUORUM \
		--use-server-timestamps true \
		--async-objects-stabilization-attempts 10 \
		--async-objects-stabilization-backoff 100ms \
		--replication-strategy "{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
		--oracle-replication-strategy "{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}" \
		--max-mutation-retries 1 \
		--max-mutation-retries-backoff 500ms \
		-c $(CONCURRENCY)
