SHELL := /bin/sh
TILT_TEST_LOG ?= /tmp/aegis_tilt_test.log
VERBOSE ?= 0

ifeq ($(VERBOSE),1)
	PYTEST_FLAGS := -s --color=yes --log-cli-level=INFO
	GOTESTSUM_FLAGS := --format standard-verbose
	AEGIS_TEST_VERBOSE := true
	AEGIS_DEBUG ?= 1
else
	PYTEST_FLAGS := --color=yes --log-cli-level=WARNING
	GOTESTSUM_FLAGS := --format testname
	AEGIS_TEST_VERBOSE := false
endif

export TILT_TEST_LOG
export VERBOSE
export AEGIS_KUBE_CONTEXT
export AEGIS_TEST_VERBOSE
export AEGIS_DEBUG

.PHONY: help lint test test-unit test-intg test-python test-go tilt-infra-up proto docs-check tf-init tf-up tf-down dev-up init-kafka

help:
	@printf '%s\n' "Aegis targets: lint test test-unit test-intg test-agent test-control-plane test-composer test-sink dev-up tilt-infra-up proto docs-check init-kafka scenario"


# ==========================================
# === TESTING & LINTING ====================
# ==========================================

lint:
	ruff check services tests
	mypy services/agent services/composer
	golangci-lint run ./...

test: test-agent test-control-plane test-composer test-sink

test-unit: test-agent-unit test-control-plane-unit test-composer-unit test-sink-unit

test-intg: test-agent-intg test-control-plane-intg test-composer-intg test-sink-intg

test-composer: test-composer-unit test-composer-intg

test-composer-unit:
	PYTHONPATH=. python3 -m pytest $(PYTEST_FLAGS) tests/unit/composer || true

test-composer-intg:
	./tests/integration/testutils/ensure_test_infra.py
	PYTHONPATH=. python3 -m pytest $(PYTEST_FLAGS) tests/integration/composer

test-agent: test-agent-unit test-agent-intg

test-agent-unit:
	PYTHONPATH=.:gen/python python3 -m pytest $(PYTEST_FLAGS) tests/unit/agent || true

test-agent-intg:
	./tests/integration/testutils/ensure_test_infra.py
	PYTHONPATH=.:gen/python python3 -m pytest $(PYTEST_FLAGS) tests/integration/agent

test-control-plane: test-control-plane-unit test-control-plane-intg

test-control-plane-unit:
	go clean -testcache
	go run gotest.tools/gotestsum@latest $(GOTESTSUM_FLAGS) -- ./...

test-control-plane-intg:
	./tests/integration/testutils/ensure_test_infra.py
	go run gotest.tools/gotestsum@latest $(GOTESTSUM_FLAGS) -- -count=1 -p 1 -tags=integration -run ^TestIntegration_ ./services/control-plane/internal/server/... ./services/control-plane/internal/incident/...

test-sink: test-sink-unit test-sink-intg

test-sink-unit:
	go run gotest.tools/gotestsum@latest $(GOTESTSUM_FLAGS) -- ./services/sink/...

test-sink-intg:
	./tests/integration/testutils/ensure_test_infra.py
	go run gotest.tools/gotestsum@latest $(GOTESTSUM_FLAGS) -- -count=1 -tags=integration -run ^TestIntegration_ ./services/sink


# ==========================================
# === TERRAFORM INFRASTRUCTURE =============
# ==========================================

tf-init:
	mise exec -- terraform -chdir=infra/terraform/local init

# Internal Base Targets
_tf-apply: tf-init
	mise exec -- terraform -chdir=infra/terraform/local workspace select -or-create $(WORKSPACE)
	mise exec -- terraform -chdir=infra/terraform/local apply -auto-approve $(if $(NODE_COUNT),-var="node_count=$(NODE_COUNT)")

_tf-destroy:
	mise exec -- terraform -chdir=infra/terraform/local workspace select $(WORKSPACE)
	mise exec -- terraform -chdir=infra/terraform/local destroy -auto-approve

_tf-kill:
	kind delete cluster --name $(CLUSTER_NAME) || true

# Environments
tf-up:
	@$(MAKE) _tf-apply WORKSPACE=default

tf-down:
	@$(MAKE) _tf-destroy WORKSPACE=default

tf-kill:
	@$(MAKE) _tf-kill CLUSTER_NAME=aegis
	rm -f infra/terraform/local/terraform.tfstate infra/terraform/local/terraform.tfstate.backup

test-tf-up:
	@$(MAKE) _tf-apply WORKSPACE=intg-test

test-tf-down:
	@$(MAKE) _tf-destroy WORKSPACE=intg-test

test-tf-kill:
	@$(MAKE) _tf-kill CLUSTER_NAME=aegis-intg-test
	rm -rf infra/terraform/local/terraform.tfstate.d/intg-test

scenario-tf-up:
	@$(MAKE) _tf-apply WORKSPACE=scenario

scenario-tf-down:
	@$(MAKE) _tf-destroy WORKSPACE=scenario

scenario-tf-kill:
	@$(MAKE) _tf-kill CLUSTER_NAME=aegis-scenario
	rm -rf infra/terraform/local/terraform.tfstate.d/scenario

tf-destroy-all: tf-down test-tf-down scenario-tf-down

tf-kill-all: tf-kill test-tf-kill scenario-tf-kill

k8s-kill:
	@echo "\033[31mNuking aegis-system namespace in local cluster to clear potentially corrupted state...\033[0m"
	@mise exec -- kubectl --context kind-aegis delete namespace aegis-system --force --grace-period=0

k8s-kill-scenario:
	@echo "\033[31mNuking aegis-system namespace in scenario cluster to clear potentially corrupted state...\033[0m"
	@mise exec -- kubectl --context kind-aegis-scenario delete namespace aegis-system --force --grace-period=0

k8s-kill-intg-test:
	@echo "\033[31mNuking aegis-system namespace in intg-test cluster to clear potentially corrupted state...\033[0m"
	@mise exec -- kubectl --context kind-aegis-intg-test delete namespace aegis-system --force --grace-period=0

k8s-kill-all: k8s-kill k8s-kill-intg-test k8s-kill-scenario


# ==========================================
# === TILT WORKLOADS =======================
# ==========================================

dev-up: tf-up
	tilt up --context kind-aegis

dev-up-0: tf-kill dev-up

dev-up-0-lite:
	kubectl --context kind-aegis delete all --all -n aegis-system --force --grace-period=0 || true
	kubectl --context kind-aegis delete pvc,configmap,secret,ingress --all -n aegis-system --force --grace-period=0 || true
	tilt up --context kind-aegis

tilt-infra-up:
	tilt up --context kind-aegis -f Tiltfile.infra

tilt-test-infra-up:
	@AEGIS_KUBE_CONTEXT=kind-aegis-intg-test $(MAKE) clear-tilt-ports
	AEGIS_ENV=intg-test tilt up --port 10352 --context kind-aegis-intg-test -f Tiltfile.infra

tilt-scenario-up:
	AEGIS_ENV=scenario AEGIS_KUSTOMIZE_OVERLAY=$(AEGIS_KUSTOMIZE_OVERLAY) mise exec -- tilt up -f Tiltfile --port 10354 --context kind-aegis-scenario


# ==========================================
# === SCENARIO RUNNER ======================
# ==========================================

scenario:
	@if [ ! -d "scripts/scenario-runner/.venv" ]; then \
		echo "Bootstrapping scenario runner environment..."; \
		python3 -m venv scripts/scenario-runner/.venv; \
		scripts/scenario-runner/.venv/bin/pip install -q -e ".[dev]"; \
	fi
	@scripts/scenario-runner/.venv/bin/python3 scripts/scenario-runner/runner.py --file $(FILE) $(ARGS)


# ==========================================
# === UTILITIES ============================
# ==========================================

# ==========================================
# === SRE CLI ==============================
# ==========================================

cli-build:
	@echo "Building SRE CLI..."
	@mkdir -p bin
	go build -o bin/aegis ./services/control-plane/cmd/aegis-cli
	@echo "Built bin/aegis"

cli:
	@if [ ! -f bin/aegis ]; then $(MAKE) cli-build; fi
	@./bin/aegis $(ARGS)

cli-dev:
	go run ./services/control-plane/cmd/aegis-cli $(ARGS)

init-kafka:
	mise exec -- kubectl $(if $(AEGIS_KUBE_CONTEXT),--context $(AEGIS_KUBE_CONTEXT)) wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n aegis-system --timeout=300s
	# Note: kubectl wait only guarantees the pod is ready inside the cluster. 
	# Tilt's local port-forwarding (which topicctl uses via localhost) is established asynchronously 
	# and might take a fraction of a second longer to bind. This retry loop handles that race condition.
	@n=0; until [ $$n -ge 10 ]; do \
		AEGIS_KAFKA_BROKER_ADDR=$(if $(AEGIS_KAFKA_BROKER_ADDR),$(AEGIS_KAFKA_BROKER_ADDR),localhost:39092) mise exec -- topicctl apply infra/kafka/topics.yaml --cluster-config infra/kafka/cluster.yaml --expand-env --skip-confirm && break; \
		n=$$((n+1)); \
		echo "Waiting for Kafka port-forward to be ready... (attempt $$n/10)"; \
		sleep 3; \
	done; \
	if [ $$n -ge 10 ]; then echo "Failed to initialize Kafka topics"; exit 1; fi

wipe-infra-state:
	@./scripts/wipe_infra_state.py $(TARGETS)

clear-tilt-ports:
	@./scripts/clear_tilt_ports.py

test-tilt-logs:
	tail -f $(TILT_TEST_LOG)

proto:
	protoc -I proto --go_out=. --go_opt=module=github.com/aegis/aegis --go-grpc_out=. --go-grpc_opt=module=github.com/aegis/aegis proto/aegis/v1/aegis.proto
	mkdir -p gen/python
	python3 -m grpc_tools.protoc -I proto --python_out=gen/python --grpc_python_out=gen/python proto/aegis/v1/aegis.proto
	touch gen/python/__init__.py
	touch gen/python/aegis/__init__.py
	touch gen/python/aegis/v1/__init__.py

docs-check:
	python3 scripts/check_topics.py
