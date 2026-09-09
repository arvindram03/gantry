# Flink with the JDBC connector, so the SQL Gateway can register a catalog
# backed by a real PostgreSQL. Without a persistent catalog the gateway's
# tables live and die with a single session, and Gantry's one-statement
# boundary leaves nowhere to create them.
FROM flink:1.20.0-scala_2.12-java17

ARG MAVEN=https://repo.maven.apache.org/maven2
ARG JDBC_CONNECTOR=3.2.0-1.19
ARG PG_DRIVER=42.7.4

RUN curl -fsSL -o /opt/flink/lib/flink-connector-jdbc.jar \
      "${MAVEN}/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar" \
 && curl -fsSL -o /opt/flink/lib/postgresql.jar \
      "${MAVEN}/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar"
