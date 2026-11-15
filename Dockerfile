# Debian 12 is the recommended base for Dataproc Serverless
FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive

# --------------------------------------------------
# 1. Install required Linux utilities
# --------------------------------------------------

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    tini \
    ca-certificates \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# 2. Create Dataproc-compatible user
# --------------------------------------------------

RUN groupadd --gid 1099 spark && \
    useradd \
        --uid 1099 \
        --gid 1099 \
        --create-home \
        --home-dir /home/spark \
        spark

# --------------------------------------------------
# 3. Application configuration
# --------------------------------------------------

ENV APP_HOME=/opt/streamline-news

ENV PYTHONPATH=/opt/streamline-news:/opt/python-packages

ENV PYTHONUNBUFFERED=1

ENV APP_ENV=gcp

ENV SPARK_EXECUTION_MODE=dataproc

# --------------------------------------------------
# 4. Create application directories
# --------------------------------------------------

RUN mkdir -p \
    ${APP_HOME} \
    /opt/python-packages \
    /opt/spark/jars

# --------------------------------------------------
# 5. Install application dependencies
# --------------------------------------------------

COPY requirements-spark.txt /tmp/requirements-spark.txt

RUN python3 -m pip install \
    --break-system-packages \
    --no-cache-dir \
    --target=/opt/python-packages \
    -r /tmp/requirements-spark.txt

# --------------------------------------------------
# 6. Install PostgreSQL JDBC driver
# --------------------------------------------------

RUN curl -fSL \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/

ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

# --------------------------------------------------
# 7. Copy application source code
# --------------------------------------------------

WORKDIR ${APP_HOME}

COPY --chown=1099:1099 src/ ./src/

# --------------------------------------------------
# 8. Set permissions
# --------------------------------------------------

RUN chmod -R a+rX ${APP_HOME} /opt/python-packages /opt/spark/jars

# Dataproc uses UID/GID 1099 at runtime.
USER 1099:1099

WORKDIR ${APP_HOME}

