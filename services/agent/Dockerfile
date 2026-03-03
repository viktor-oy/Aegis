FROM python:3.13.13-slim-bookworm

WORKDIR /workspace
COPY pyproject.toml ./
COPY services/agent ./services/agent
RUN python -m pip install --upgrade pip==24.3.1 \
    && python -m pip install -e . \
    && useradd -m -u 10001 aegis

ENV PYTHONPATH=/workspace/services/agent
USER 10001
ENTRYPOINT ["python", "-m", "aegis_agent"]

