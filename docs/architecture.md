# NATS-Flink Connector Architecture

The NATS-Flink Connector integrates Apache Flink with NATS and JetStream to enable seamless data processing in distributed systems. It provides a robust framework for lightweight streaming (NATS Core) and stateful, durable messaging (JetStream). 

---

## Modes of Operation

The connector supports two modes:
1. **NATS Core Mode**:
   - Handles basic, ephemeral messaging without persistence.
   - Ideal for lightweight, stateless streaming applications.

2. **NATS JetStream Mode (Source Only)**:
   - Streams durable messages from JetStream into Flink pipelines.
   - Enables replay, acknowledgment, and checkpoint integration for fault tolerance.

---


## NATS Core Mode

### Data Flow
1. Messages are published to NATS subjects.
2. **NATS Source**:
   - Creates splits based on subjects.
   - Each split is assigned to a Flink task.
   - Deserializes payloads into Flink-readable formats.
3. **Flink Pipeline**:
   - Processes the messages using user-defined transformations.
4. **NATS Sink**:
   - Publishes the processed messages back to specified subjects.

### Features
- Stateless processing.
- High-throughput, low-latency streaming.
- Customizable serialization/deserialization.

---

## NATS JetStream Mode (Source Connector)

### Data Flow
1. Messages are published to JetStream streams with durability.
2. **JetStream Source**:
   - Enumerates subjects into splits.
   - Reads messages with replay capabilities.
   - Acknowledges processed messages after successful checkpointing.
3. **Flink Pipeline**:
   - Processes messages while ensuring exactly-once or at-least-once semantics.

---

## Core Components

### 1. Source Connector
The Source Connector leverages Flink's new Source API, which includes:
- **Split Enumerator**:
  - Divides NATS subjects into manageable splits.
  - Dynamically assigns splits to source readers based on parallelism.
- **Split Reader**:
  - Handles fetching messages for a specific split.
  - Converts raw messages into deserialized objects for processing.
- **Checkpointing**:
  - Captures split state to enable fault tolerance and recovery.
  - In JetStream mode, ensures that only acknowledged messages are checkpointed.

#### Relevant Classes
- **Split Management**:
  - `NatsSubjectSplit`: Represents a single unit of work (a NATS subject).
  - `NatsSubjectSplitSerializer`: Serializes split data for state persistence.
- **Readers**:
  - `NatsSourceReader`: Manages split readers and emits data to Flink.
  - `NatsJetStreamSourceReader`: Extends `NatsSourceReader` for JetStream-specific logic.
  - `NatsSubjectSplitReader`: Fetches messages for a specific split.

### 2. Sink Connector
- Publishes processed data back to NATS Core subjects.
- **Note**: JetStream support for Sink Connector is not implemented.

#### Relevant Classes
- `NatsSink`: Handles data serialization and publishing to NATS.
- `NatsSinkBuilder`: Configures the sink properties.

### 3. Utilities
- **Connection Management**:
  - Simplifies configuration and management of NATS connections.
- **Serialization/Deserialization**:
  - Supports custom payload transformations.
- **Checkpointing**:
  - Integrates with Flink's state API for fault-tolerant processing.

---

## Exactly Once Semantics (JetStream Source)

### Message Acknowledgment
- Messages are consumed and processed in a checkpointed Flink pipeline.
- Only after a successful checkpoint, messages are acknowledged in JetStream.

### Split Recovery
- In case of a failure, split states are recovered from checkpoints.
- The JetStream Source replays messages from the last unacknowledged state.

---

## Diagram
*to be decided*

---


## Future Enhancements

1. **JetStream Sink Support**:
   - Enable durable publishing to JetStream streams.
2. **Enhanced Monitoring**:
   - Metrics for split processing, acknowledgment delays, and JetStream performance.
3. **Multiple Consumers**:
   - Support for parallel consumers in JetStream mode.
4. **Batch Acknowledgement post Flink Job**:
    - Acknowledge messages in bulk after successful Flink job completion.

---