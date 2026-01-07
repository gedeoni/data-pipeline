FROM apache/superset:latest

USER root

# Install drivers into the virtual environment used by Superset.
# We target the specific site-packages directory because the global pip doesn't write there by default.
RUN pip install --no-cache-dir --target=/app/.venv/lib/python3.10/site-packages psycopg2-binary clickhouse-connect

USER superset