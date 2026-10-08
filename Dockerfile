# syntax=docker/dockerfile:1.7
# One image for all three Dagster processes (code location, webserver, daemon).

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app

# Dependencies first, so code changes do not invalidate this layer.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra webserver --no-install-project

COPY src ./src
COPY dbt ./dbt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra webserver

# Build the dbt manifest at image build time; dagster-dbt loads it on startup.
ENV TRAVEL_DATA_DIR=/data \
    TRAVEL_DUCKDB_PATH=/data/warehouse/travel.duckdb \
    DBT_TARGET=prod \
    DAGSTER_HOME=/opt/dagster/home
RUN cd dbt && dbt parse --profiles-dir . --target prod \
    && rm -rf logs

RUN useradd --create-home --uid 10001 dagster \
    && mkdir -p /data /opt/dagster/home \
    && chown -R dagster:dagster /data /opt/dagster /app/dbt
COPY --chown=dagster:dagster docker/dagster.yaml docker/workspace.yaml /opt/dagster/home/

USER dagster
VOLUME ["/data"]
EXPOSE 3000 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "travel_pipeline.definitions"]
