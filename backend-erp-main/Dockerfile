FROM eclipse-temurin:21-jdk-jammy AS erp-api-template-java-base

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends \
      vim tar gzip && \
    rm -rf /var/lib/apt/lists/*

FROM erp-api-template-java-base AS erp-api-template-back
ARG ACTIVE_PROFILE
ARG FULL_TEST
WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle settings.gradle /app/

RUN ./gradlew build -x test --stacktrace > /dev/null 2>&1 || true

COPY . /app

RUN chmod +x gradlew
RUN ./gradlew build -Pprofile=${ACTIVE_PROFILE} -Pfull=${FULL_TEST}

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=erp-api-template-back /app/build/libs/*-SNAPSHOT.jar app.jar

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]