# Superset + driver ClickHouse. Build mot lan, metadata Superset nam trong volume.
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir clickhouse-connect psycopg2-binary
USER superset
