.PHONY: generate clients build test lint fmt vet workflowcheck ci

# nexgen is downloaded from its GitHub release into .build/ the first time it is needed.
NEXGEN_VERSION := 0.2.9
NEXGEN_OS := $(if $(filter Darwin,$(shell uname -s)),apple-darwin,unknown-linux-gnu)
NEXGEN_ARCH := $(if $(filter arm64 aarch64,$(shell uname -m)),aarch64,x86_64)
NEXGEN := .build/nexgen-$(NEXGEN_VERSION)

$(NEXGEN):
	mkdir -p .build
	curl --silent --show-error --fail --location https://github.com/temporalio/nexgen/releases/download/v$(NEXGEN_VERSION)/nexgen-v$(NEXGEN_VERSION)-$(NEXGEN_ARCH)-$(NEXGEN_OS).tar.gz | tar --extract --gzip --directory .build nexgen
	mv .build/nexgen $@

# nexgen never deletes files, so a stale one from an earlier run would stay behind.
generate: $(NEXGEN)
	rm -rf generated/kafkaapi
	$(NEXGEN) go kafka.nexusrpc.yaml --output generated/kafkaapi
	gofmt -w generated/kafkaapi

# Generates callers in every language nexgen supports, to show what they look like. Not committed.
clients: $(NEXGEN)
	rm -rf .build/clients
	$(NEXGEN) go kafka.nexusrpc.yaml --output .build/clients/go/kafkaapi
	$(NEXGEN) python kafka.nexusrpc.yaml --output .build/clients/python/kafkaapi
	$(NEXGEN) typescript kafka.nexusrpc.yaml --output .build/clients/typescript/kafkaapi
	$(NEXGEN) java kafka.nexusrpc.yaml --output .build/clients/java/kafkaapi --package-name com.example.kafkaapi

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
