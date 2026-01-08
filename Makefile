SHELL := /bin/sh

.PHONY: help lint test test-python test-go kind-up kind-down tilt-up proto docs-check

help:
	@printf '%s\n' "Aegis targets: lint test test-python test-go kind-up tilt-up proto docs-check"

lint:
	ruff check services tests
	mypy services/agent services/composer
	golangci-lint run ./...

test: test-python test-go

test-python:
	PYTHONPATH=services/agent:services/composer python3 -m unittest discover -s tests/unit -p 'test_*.py'

test-go: test-go-unit test-go-intg

test-go-unit:
	go clean -testcache
	go test ./...

test-go-intg:
	go test ./services/control-plane/internal/server/grpc_integration_test.go

kind-up:
	@echo "Starting local registry..."
	@if ! docker inspect kind-registry > /dev/null 2>&1; then \
		docker run -d --restart=always -p 127.0.0.1:5001:5000 --name kind-registry registry:2.8.3; \
	else \
		echo "Registry already running."; \
	fi
	@echo "Creating kind cluster..."
	kind create cluster --name aegis --image kindest/node:v1.34.8 --config infra/kind/cluster.yaml
	@echo "Connecting registry to kind network..."
	@docker network connect kind kind-registry 2>/dev/null || true
	@echo "Annotating nodes for local registry discovery..."
	for node in $$(kind get nodes --name aegis); do \
		kubectl annotate node "$$node" tilt.dev/registry=localhost:5001 --overwrite; \
	done

kind-down:
	kind delete cluster --name aegis
	docker rm -f kind-registry 2>/dev/null || true

tilt-up:
	tilt up

proto:
	protoc -I proto --go_out=. --go_opt=module=github.com/aegis/aegis --go-grpc_out=. --go-grpc_opt=module=github.com/aegis/aegis proto/aegis/v1/aegis.proto

docs-check:
	python3 scripts/check_topics.py

