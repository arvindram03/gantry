# SPDX-License-Identifier: Apache-2.0
#
# The image a `beam` job runs in.
#
# Three things the official Beam SDK image does not give us, and all three are
# required rather than convenient:
#
#   1. A JRE. `apache_beam.io.jdbc` is a cross-language transform: the Python
#      API is a facade over a Java expansion service. The official image ships
#      no Java, so JdbcIO cannot run in it at all.
#   2. The expansion service JAR, staged at build time.
#   3. The PostgreSQL JDBC driver, likewise.
#
# Staging the JARs is not hardening. Left to itself Beam downloads both from
# Maven Central *at execution time*, in the process holding the database
# credentials, and prints its own warning while doing so. A job that must not
# reach the public internet while running cannot be built any other way.
#
# Versions are pinned because a jar that changes underneath a replay is exactly
# what the job's content hash exists to make visible.

FROM apache/beam_python3.12_sdk:2.76.0

ARG BEAM_VERSION=2.76.0
ARG PG_JDBC_VERSION=42.2.16
ARG MAVEN=https://repo.maven.apache.org/maven2

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Beam looks here before it reaches for the network.
RUN mkdir -p /root/.apache_beam/cache/jars \
 && curl -fsSL -o "/root/.apache_beam/cache/jars/beam-sdks-java-extensions-schemaio-expansion-service-${BEAM_VERSION}.jar" \
      "${MAVEN}/org/apache/beam/beam-sdks-java-extensions-schemaio-expansion-service/${BEAM_VERSION}/beam-sdks-java-extensions-schemaio-expansion-service-${BEAM_VERSION}.jar" \
 && curl -fsSL -o "/root/.apache_beam/cache/jars/postgresql-${PG_JDBC_VERSION}.jar" \
      "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

# The SDK image's entrypoint is the worker boot binary, which expects to be
# handed an id by a runner. A Gantry job supplies its own argv, so the
# entrypoint is cleared rather than replaced with a guess at one.
ENTRYPOINT []
