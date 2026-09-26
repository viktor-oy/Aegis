FROM ollama/ollama:0.7.1

# Model to bake at build time (cache warm-up). Override at runtime with:
#   docker run -e OLLAMA_MODEL=qwen2.5:7b ...
# On Apple Silicon (M1/M2/M3) Ollama uses Metal GPU acceleration automatically.
ARG OLLAMA_MODEL=qwen2.5:0.5b
ENV OLLAMA_MODEL=${OLLAMA_MODEL}


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 11434
ENTRYPOINT ["/entrypoint.sh"]
