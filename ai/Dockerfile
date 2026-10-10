# ai/Dockerfile
# Build context must be ai/, e.g.:
#   docker build -t sage-ai ai/
#
# Uses python:3.12-slim for both stages rather than
# gcr.io/distroless/python3-debian12 (the rest of this project's usual
# distroless pattern, see backend/cmd/*/Dockerfile) — distroless's bundled
# Python version isn't pinned to a version this Dockerfile controls, which
# risks a bytecode/ABI mismatch against compiled dependencies in the
# langchain/pydantic stack built in the separate builder stage. slim keeps
# the builder and runtime stages on the exact same interpreter.

FROM python:3.12-slim AS builder
WORKDIR /src
COPY pyproject.toml ./
COPY ai ./ai
RUN pip install --no-cache-dir --target=/deps .

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /deps /usr/local/lib/python3.12/site-packages
COPY --from=builder /src/ai ./ai
RUN useradd --uid 65532 --no-create-home nonroot
USER nonroot
ENTRYPOINT ["python", "-m", "ai"]
