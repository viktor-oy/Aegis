SHELL := /bin/sh

.PHONY: help lint test test-python test-go kind-up kind-down tilt-up proto docs-check

help:
	@printf '%s\n' "Aegis targets: lint test test-agent test-control-plane test-composer test-sink kind-up tilt-up proto docs-check"

lint:
	ruff check services tests
	mypy services/agent services/composer
	golangci-lint run ./...

test: test-agent test-control-plane test-composer test-sink

test-composer: test-composer-unit test-composer-intg

test-composer-unit:
	PYTHONPATH=. python3 -m pytest tests/unit/composer || true

test-composer-intg:
	PYTHONPATH=. python3 -m pytest tests/integration/composer

test-agent: test-agent-unit test-agent-intg

test-agent-unit:
	PYTHONPATH=.:gen/python python3 -m pytest tests/unit/agent || true

test-agent-intg:
	PYTHONPATH=.:gen/python python3 -m pytest tests/integration/agent

test-control-plane: test-control-plane-unit test-control-plane-intg

test-control-plane-unit:
	go clean -testcache
	go test ./...

test-control-plane-intg:
	go test ./services/control-plane/internal/server/server_integration_test.go

test-sink: test-sink-unit test-sink-intg

test-sink-unit:
	go test ./services/sink/...

test-sink-intg:
	go test -v -tags=integration ./services/sink/sink_integration_test.go ./services/sink/main.go

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
	mkdir -p gen/python
	python3 -m grpc_tools.protoc -I proto --python_out=gen/python --grpc_python_out=gen/python proto/aegis/v1/aegis.proto
	touch gen/python/__init__.py
	touch gen/python/aegis/__init__.py
	touch gen/python/aegis/v1/__init__.py

docs-check:
	python3 scripts/check_topics.py

