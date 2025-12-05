# Stage 1: Build the application using Maven
FROM maven:3.9-amazoncorretto-11 AS builder

WORKDIR /app

# because of the issue with the jdk.tools dependency, we need to create a dummy tools.jar file
RUN mkdir -p "${JAVA_HOME}/../lib"  && touch "${JAVA_HOME}/../lib/tools.jar"

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:go-offline

# Copy the source code and build the application
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package

# Stage 2: Create the runtime image
FROM amazoncorretto:11-alpine-jdk

WORKDIR /app

# Copy the JAR from the builder stage
COPY --from=builder /app/target/thrift-jdbc-test-1.0-SNAPSHOT.jar ./app.jar

# Command to run the application
# The SPARK_THRIFT_HOST environment variable can be passed during 'docker run'
# e.g., docker run -e SPARK_THRIFT_HOST=spark_standalone_cluster ...
ENTRYPOINT ["java", "-jar", "./app.jar"] 