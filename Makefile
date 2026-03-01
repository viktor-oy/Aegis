SHELL := /bin/sh
TILT_TEST_LOG ?= /tmp/aegis_tilt_test.log
export TILT_TEST_LOG

.PHONY: help lint test test-unit test-intg test-python test-go tilt-infra-up proto docs-check tf-init tf-up tf-down dev-up init-kafka

help:
	@printf '%s\n' "Aegis targets: lint test test-unit test-intg test-agent test-control-plane test-composer test-sink dev-up tilt-infra-up proto docs-check init-kafka"

lint:
	ruff check services tests
	mypy services/agent services/composer
	golangci-lint run ./...

test: test-agent test-control-plane test-composer test-sink

test-unit: test-agent-unit test-control-plane-unit test-composer-unit test-sink-unit

test-intg: test-agent-intg test-control-plane-intg test-composer-intg test-sink-intg

test-composer: test-composer-unit test-composer-intg

test-composer-unit:
	PYTHONPATH=. python3 -m pytest -s --log-cli-level=INFO tests/unit/composer || true

test-composer-intg:
	./tests/integration/testutils/ensure_test_infra.py
	PYTHONPATH=. python3 -m pytest -s --color=yes --log-cli-level=INFO tests/integration/composer

test-agent: test-agent-unit test-agent-intg

test-agent-unit:
	PYTHONPATH=.:gen/python python3 -m pytest -s --color=yes --log-cli-level=INFO tests/unit/agent || true

test-agent-intg:
	./tests/integration/testutils/ensure_test_infra.py
	PYTHONPATH=.:gen/python python3 -m pytest -s --color=yes --log-cli-level=INFO tests/integration/agent

test-control-plane: test-control-plane-unit test-control-plane-intg

test-control-plane-unit:
	go clean -testcache
	go run gotest.tools/gotestsum@latest --format standard-verbose -- ./...

test-control-plane-intg:
	./tests/integration/testutils/ensure_test_infra.py
	go run gotest.tools/gotestsum@latest --format standard-verbose -- ./services/control-plane/internal/server/server_integration_test.go

test-sink: test-sink-unit test-sink-intg

test-sink-unit:
	go run gotest.tools/gotestsum@latest --format standard-verbose -- ./services/sink/...

test-sink-intg:
	./tests/integration/testutils/ensure_test_infra.py
	go run gotest.tools/gotestsum@latest --format standard-verbose -- -tags=integration ./services/sink


tilt-infra-up:
	tilt up -f Tiltfile.infra

tilt-test-infra-up: clear-test-ports
	AEGIS_ENV=intg-test tilt up --port 10352 --context kind-aegis-intg-test -f Tiltfile.infra

KUBE_CONTEXT ?= kind-aegis

init-kafka:
	kubectl --context $(KUBE_CONTEXT) wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n aegis-system --timeout=300s
	topicctl apply infra/kafka/topics.yaml --cluster-config infra/kafka/cluster.yaml --skip-confirm

wipe-infra-state:
	@./scripts/wipe_infra_state.py $(TARGETS)

clear-test-ports:
	@./scripts/clear_test_ports.py

test-tilt-logs:
	tail -f $(TILT_TEST_LOG)



tf-init:
	mise exec -- terraform -chdir=infra/terraform/local init

_tf-apply: tf-init
	mise exec -- terraform -chdir=infra/terraform/local workspace select -or-create $(WORKSPACE)
	mise exec -- terraform -chdir=infra/terraform/local apply -auto-approve

tf-up:
	@$(MAKE) _tf-apply WORKSPACE=default

test-tf-up:
	@$(MAKE) _tf-apply WORKSPACE=intg-test

tf-down:
	mise exec -- terraform -chdir=infra/terraform/local destroy -auto-approve

tf-kill:
	kind delete cluster --name aegis || true
	rm -f infra/terraform/local/terraform.tfstate*
	rm -rf infra/terraform/local/.terraform
	rm -f infra/terraform/local/.terraform.lock.hcl

dev-up: tf-up
	tilt up

dev-up-0: tf-kill dev-up

dev-up-0-lite:
	kubectl delete all --all -n aegis-system --force --grace-period=0 || true
	kubectl delete pvc,configmap,secret,ingress --all -n aegis-system --force --grace-period=0 || true
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

