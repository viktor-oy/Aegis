FROM python:3.13.13-slim-bookworm

WORKDIR /workspace
COPY pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    mkdir -p services/agent gen/python && touch services/__init__.py gen/__init__.py \
    && python -m pip install --upgrade pip==24.3.1 \
    && python -m pip install -e . \
    && useradd -m -u 10001 aegis

COPY services/agent ./services/agent
COPY services/pkg ./services/pkg
COPY gen/python ./gen/python

ENV PYTHONPATH=/workspace:/workspace/gen/python
ENV AEGIS_CP_BOOTSTRAP_URLS=
ENV AEGIS_WORKER_ID=
ENV AEGIS_SIMULATION_MODE=
ENV AEGIS_TELEMETRY_DATA_SOURCE=
ENV AEGIS_AGENT_API_PORT=
ENV AEGIS_HEARTBEAT_INTERVAL_SECONDS=

USER 10001
ENTRYPOINT ["python", "-m", "services.agent.main"]
