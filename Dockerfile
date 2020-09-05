FROM maven:3.6.3-jdk-11 AS builder

COPY . /tmp/build/app
WORKDIR /tmp/build/app
RUN mvn package -DskipTests

FROM openjdk:11-jre

COPY --from=builder /tmp/build/app/target/kstream-product-merge-example-*.jar /usr/src/app/app.jar
WORKDIR /usr/src/app

ENV JAVA_OPTS ""
CMD ["bash", "-c", "java $JAVA_OPTS -jar app.jar"]
