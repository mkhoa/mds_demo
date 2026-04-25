FROM pgduckdb/pgduckdb:17-main

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-pgvector postgis \
    && rm -rf /var/lib/apt/lists/*
USER postgres
