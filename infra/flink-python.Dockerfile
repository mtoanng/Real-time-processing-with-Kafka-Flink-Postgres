FROM maven:3.9.9-eclipse-temurin-17 AS connector-build

WORKDIR /build
COPY pom.xml ./
COPY flink-connectors/pom.xml flink-connectors/pom.xml
RUN mvn -B -q -pl flink-connectors -am package -DskipTests

FROM flink:2.2.1-scala_2.12-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-venv \
    && python3 -m venv /opt/pyflink \
    && /opt/pyflink/bin/pip install --no-cache-dir apache-flink==2.2.1 \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/opt/pyflink/bin:${PATH}"
COPY --from=connector-build \
    /build/flink-connectors/target/taobao-flink-connectors.jar \
    /opt/flink/lib/taobao-flink-connectors.jar
USER flink
