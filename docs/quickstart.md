
# Quickstart Guide

Welcome to the **NATS-Flink Connector** Quickstart guide! This document will help you set up and start using the connector with either **NATS Core** or **NATS JetStream**.

---
## Prerequisites
Before you begin, ensure you have the following:
1. **NATS Server** (v2.9 or later).
2. **Apache Flink** (v1.13 or later).
3. **Java Development Kit (JDK)** (v11 or later).
4. **Maven** or **Gradle** for building Java projects.

---

## Part 1: Quickstart with `NATS Core`

### Step 1: Setup

1. Start your NATS server:
    ```bash
    nats-server
    ```

2. Add the connector dependency to your project:

#### Maven
```xml
<dependency>
    <groupId>io.synadia</groupId>
    <artifactId>flink-connector-nats</artifactId>
    <version>{latest.version}</version>
</dependency>
````

#### Gradle

```groovy
dependencies {
    implementation 'io.synadia:flink-connector-nats:{latest.version}'
}
```

* * *

### Step 2: Example Code

#### Source: Read from NATS

```java
NatsSource<String> source = new NatsSourceBuilder<String>()
        .sourceProperties("/path/to/application.properties")
        .connectionProperties("/path/to/connection.properties")
        .build();

env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "NATS Core Source"
).print();
```

#### Sink: Write to NATS

```java
 NatsSink<String> sink = new NatsSinkBuilder<String>()
        .sinkProperties("/path/to/application.properties")
        .connectionProperties("/path/to/application.properties")
        .build();

env.sinkTo(sink);
```
* * *

Step 3: Full Example Reference
------------------------------

For a complete implementation, refer to the `SourceToSinkExample.java` file in the repository.

* * *

Part 2: Quickstart with `NATS JetStream`
--------------------------------------

JetStream provides advanced features like durable streams, message replay, and acknowledgment management.

### Step 1: Enable JetStream

Update your NATS server configuration (`nats-server.conf`):

```conf
jetstream: enabled
server_name: MyNatsServer
listen: 0.0.0.0:4222
```

Start the server:

```bash
nats-server -c /path/to/nats-server.conf
```

* * *

### Step 2: Example Code

#### JetStream Source: Durable and Stateful

```java
NatsJetStreamSource<String> source = new NatsJetStreamSourceBuilder<String>()
    .subjects("example.subject")
    .payloadDeserializer("io.synadia.payload.StringPayloadDeserializer")
    .connectionProperties("/path/to/connection.properties")
    .consumerName("example-consumer")
    .build();

env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "NATS JetStream Source"
).print();
```

#### Core Sink:

```java
NatsSink<String> sink = new NatsSinkBuilder<String>()
    .subjects("example.output.subject")
    .connectionPropertiesFile("/path/to/connection.properties")
    .payloadSerializerClass("io.synadia.payload.StringPayloadSerializer")
    .build();

env.addSink(sink);
```

* * *

Step 3: Full Example Reference
------------------------------

For a complete implementation, refer to the `SourceToSinkJsExample.java` file in the repository.

* * *