FROM apache/superset:latest

USER root

RUN /app/.venv/bin/python -m ensurepip

RUN /app/.venv/bin/python -m pip install --upgrade pip

RUN /app/.venv/bin/python -m pip install \
    duckdb \
    duckdb-engine \
    psycopg2-binary

USER superset