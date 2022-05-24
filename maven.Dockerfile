FROM maven:3.8.5-eclipse-temurin-17
WORKDIR /app
COPY pom.xml .
COPY ./cacerts /opt/java/openjdk/lib/security/cacerts
RUN mvn clean install
COPY src src
COPY dataRefresh dataRefresh
RUN mvn package
ENTRYPOINT ["java","-jar","target/ETL_DataRefresh_POC-1.0-SNAPSHOT.jar"]