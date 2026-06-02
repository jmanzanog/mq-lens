# Stage 1: Build the plugin
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build
COPY activemq-plugin/pom.xml .
COPY activemq-plugin/src ./src
RUN mvn clean package -DskipTests

# Stage 2: Build the final ActiveMQ image
FROM apache/activemq-classic:5.18.3

# Copy the plugin JAR to the ActiveMQ lib directory
COPY --from=builder /build/target/mq-lens-activemq-plugin-*.jar /opt/apache-activemq/lib/mq-lens-plugin.jar

# Provide our pre-configured activemq.xml with the plugin injected
COPY docker/activemq/activemq.xml /opt/apache-activemq/conf/activemq.xml

# Expose typical ports
EXPOSE 8161 61616 5672 61613 1883 61614

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl -f http://localhost:8161/api/jolokia/version/GET || exit 1
