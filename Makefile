.PHONY: generate build test lint fmt vet workflowcheck ci

# nexgen is downloaded from its GitHub release into .build/ the first time it is needed.
NEXGEN_VERSION := 0.2.9
NEXGEN_OS := $(if $(filter Darwin,$(shell uname -s)),apple-darwin,unknown-linux-gnu)
NEXGEN_ARCH := $(if $(filter arm64 aarch64,$(shell uname -m)),aarch64,x86_64)
NEXGEN := .build/nexgen-$(NEXGEN_VERSION)

$(NEXGEN):
	mkdir -p .build
	curl -sSfL https://github.com/temporalio/nexgen/releases/download/v$(NEXGEN_VERSION)/nexgen-v$(NEXGEN_VERSION)-$(NEXGEN_ARCH)-$(NEXGEN_OS).tar.gz | tar -xz -C .build nexgen
	mv .build/nexgen $@

# nexgen never deletes files, so a stale one from an earlier run would stay behind.
generate: $(NEXGEN)
	rm -rf generated/kafkaapi
	$(NEXGEN) go kafka.nexusrpc.yaml --output generated/kafkaapi
	gofmt -w generated/kafkaapi

build:
	go build ./...

test:
	go test -race -count=1 ./...

fmt:
	golangci-lint run --fix --timeout=5m

vet:
	go vet ./...

workflowcheck:
	go vet -vettool=$$(go env GOPATH)/bin/workflowcheck ./...

lint:
	golangci-lint run --timeout=5m

ci: vet workflowcheck lint test
