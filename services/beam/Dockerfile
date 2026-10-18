FROM apache/beam_python3.11_sdk:2.77.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir psycopg2-binary==2.9.9

WORKDIR /app
COPY services/beam/src/ ./src/
COPY services/beam/tests/ ./tests/
COPY services/beam/start.sh /usr/local/bin/beam-entrypoint
RUN chmod +x /usr/local/bin/beam-entrypoint

ENTRYPOINT ["/usr/local/bin/beam-entrypoint"]
