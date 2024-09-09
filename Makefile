MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

GO111MODULE := on
# GO_UPGRADE - do not remove this comment, used by scripts/go-upgrade.sh
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)

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

.PHONY: check-golangci
check-golangci: $(GOBIN)/golangci-lint
	$(GOBIN)/golangci-lint run

# fix-golangci Automated fix for golangci-lint errors.
.PHONY: fix-golangci
fix-golangci: $(GOBIN)/golangci-lint
	$(GOBIN)/golangci-lint run --fix

# check Run all static code analysis. (use make fix to attempt automatic fix)
.PHONY: check
check: check-golangci

# fix make all static code analysis tools to fix the issues
.PHONY: fix
fix: fix-golangci

.PHONY: test
test:
	go test -covermode=atomic -race -coverprofile=coverage.txt -timeout 5m -json -v ./... 2>&1 | gotestfmt -showteststatus

.PHONY: build
build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/gemini ./cmd/gemini

debug-build:
	CGO_ENABLED=0 go build -gcflags "all=-N -l" -o bin/gemini ./cmd/gemini

.PHONY: build-docker
build-docker:
	docker build --target production -t scylladb/gemini:latest --compress .
