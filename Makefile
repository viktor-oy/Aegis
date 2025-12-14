SHELL := /bin/sh

.PHONY: help lint test test-python test-go kind-up tilt-up proto docs-check

help:
	@printf '%s\n' "Aegis targets: lint test test-python test-go kind-up tilt-up proto docs-check"

lint:
	ruff check services tests
	mypy services/agent services/composer
	golangci-lint run ./...

test: test-python test-go

test-python:
	PYTHONPATH=services/agent:services/composer python3 -m unittest discover -s tests/unit -p 'test_*.py'

test-go:
	go test ./...

kind-up:
	kind create cluster --name aegis --image kindest/node:v1.34.8

tilt-up:
	tilt up

proto:
	protoc -I proto --go_out=. --go-grpc_out=. --python_out=services/agent --grpc_python_out=services/agent proto/aegis/v1/aegis.proto

docs-check:
	python3 scripts/check_topics.py

