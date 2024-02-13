FROM eclipse-temurin:17.0.7_7-jdk-alpine
MAINTAINER paul.nthusi@thepalladiumgroup.com
COPY target/superset-api-layer-0.0.1-SNAPSHOT.jar superset-api-layer-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","/superset-api-layer-0.0.1-SNAPSHOT.jar"]