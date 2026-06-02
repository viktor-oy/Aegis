FROM python:3.13.13-slim-bookworm

WORKDIR /workspace
COPY pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    mkdir -p services/composer gen/python && touch services/__init__.py gen/__init__.py \
    && python -m pip install --upgrade pip==24.3.1 \
    && python -m pip install -e . \
    && useradd -m -u 10001 aegis

COPY services/composer ./services/composer
COPY services/pkg ./services/pkg

ENV PYTHONPATH=/workspace

ENV AEGIS_KAFKA_BOOTSTRAP_SERVERS=""
ENV AEGIS_COMPOSER_GROUP_ID=""
ENV AEGIS_COMPOSER_INPUT_TOPIC=""
ENV AEGIS_COMPOSER_OUTPUT_TOPIC=""
ENV AEGIS_LLM_BASE_URL=""
ENV AEGIS_LLM_MODEL=""
ENV AEGIS_LLM_API_KEY=""
ENV AEGIS_LLM_TIMEOUT_SECONDS=""
ENV AEGIS_LLM_TEMPERATURE=""
ENV AEGIS_LLM_MAX_TOKENS=""
ENV AEGIS_COMPOSER_SYS_ARCH_PROMPT=""
ENV AEGIS_COMPOSER_FORMAT_PROMPT=""

USER 10001
ENTRYPOINT ["python", "-m", "services.composer"]

