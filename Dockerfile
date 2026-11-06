# One image for every role: NODE_ROLE=manager|worker|submit decides what runs.
# Build the jar first:  mvn package -DskipTests
FROM docker.io/library/eclipse-temurin:21-jre
COPY target/agenor-example-job-dispatcher-0.1.0-SNAPSHOT-service.jar /app/service.jar
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
