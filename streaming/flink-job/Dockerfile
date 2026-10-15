FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

FROM flink:1.20.2-scala_2.12-java17
USER root
RUN mkdir -p /flink-data/checkpoints && chown -R flink:flink /flink-data
COPY --from=build --chown=flink:flink /build/target/fraud-detection-streaming-job.jar /opt/flink/usrlib/fraud-detection-streaming-job.jar
USER flink
