# Use a multi-stage build to keep the final image clean
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the shaded JAR
RUN mvn clean package -DskipTests

# Run stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/target/spanner-cdc-loadtest-1.0-SNAPSHOT.jar app.jar

# Entry point allows passing arguments directly to the JAR
ENTRYPOINT ["java", "-jar", "app.jar"]
