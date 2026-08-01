FROM maven:3.9.9-eclipse-temurin-11 AS connector-build

WORKDIR /build
COPY pom.xml ./
COPY flink-connectors/pom.xml flink-connectors/pom.xml
RUN mvn -B -q -pl flink-connectors -am package -DskipTests

FROM flink:1.20.2-scala_2.12-java11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==1.20.2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=connector-build \
    /build/flink-connectors/target/taobao-flink-connectors.jar \
    /opt/flink/lib/taobao-flink-connectors.jar
USER flink
