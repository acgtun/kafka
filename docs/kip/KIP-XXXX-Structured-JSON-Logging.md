# KIP-XXXX: Structured JSON Logging for Apache Kafka

## Status

**Current State**: Draft

**Discussion Thread**: (link)

**JIRA**: (link)

**Released**: N/A

## Motivation

Apache Kafka currently outputs all log messages as unstructured plain text using a
simple pattern: `[%d] %p %m (%c)%n`. For example:

```
[2026-05-12 10:00:00,000] INFO [BrokerServer id=0] Started broker server (kafka.server.BrokerServer)
```

While human-readable, this format has significant limitations:

1. **Machine parsing is fragile**: Context fields like broker ID, topic, partition, and
   client ID are embedded in a free-form string prefix (`[BrokerServer id=0]`). Parsing
   them requires regex heuristics that break when prefix formats vary across components.

2. **AI/LLM-based debugging tools struggle**: Modern AI-powered log analysis, anomaly
   detection, and automated root-cause-analysis tools work orders of magnitude better
   with structured data. An AI tool that receives `{"brokerId":0, "topic":"payments",
   "partition":3}` can instantly filter and correlate; one that receives
   `[UnifiedLog partition=payments-3, dir=/data/kafka]` must first guess the field format.

3. **Log aggregation is lossy**: When ingesting Kafka logs into Elasticsearch,
   Splunk, Datadog, or Loki, operators must write custom grok/regex parsers for each
   component's prefix format. These parsers are brittle and lose information when the
   format changes.

4. **No MDC context fields**: The SLF4J Mapped Diagnostic Context (MDC) is unused by
   the broker, controller, and client libraries (only Kafka Connect uses it for
   `connector.context`). This means structured logging layouts like
   `JsonTemplateLayout` produce JSON with empty context fields.

5. **Cross-component correlation is difficult**: There are no shared context fields
   (like `kafka.node.id` or `kafka.cluster.id`) that appear consistently across all log
   messages from a given broker, making it hard to filter logs by source.

### Industry context

Structured logging is industry standard. Kubernetes components output JSON logs by
default (KEP-1602, GA since 1.24). Elasticsearch, MongoDB, PostgreSQL, and most
modern infrastructure software provide structured JSON log modes. Apache Kafka is a
notable holdout.

## Public Interfaces

### 1. `LogContext` — New Constructor and Method

`org.apache.kafka.common.utils.internals.LogContext` gains a new public constructor
and accessor:

```java
/**
 * Create a LogContext with both a human-readable prefix and a structured context map.
 *
 * @param logPrefix   the string prefix prepended to every log message (may be null)
 * @param contextMap  structured key-value pairs pushed to SLF4J MDC on each log call
 */
public LogContext(String logPrefix, Map<String, String> contextMap)

/** Returns the unmodifiable structured context map. */
public Map<String, String> contextMap()
```

The existing constructors `LogContext()` and `LogContext(String)` are **unchanged** and
continue to produce loggers with no MDC behavior (empty context map).

**Note:** `LogContext` lives in the `internals` package and is not part of the official
public API contract. However, it is widely used by Kafka internals and downstream
projects that embed Kafka components, so the change is listed here for completeness.

### 2. Standard MDC Key Names (Log Output Contract)

When JSON logging is enabled, the following MDC keys appear as top-level JSON fields.
These key names become a **stable public contract** — log consumers (dashboards, alerts,
AI tools) will depend on them.

| MDC Key | Type | Description |
|---|---|---|
| `kafka.node.id` | string | Broker or controller node ID |
| `kafka.cluster.id` | string | Kafka cluster ID |
| `kafka.component` | string | Component name (e.g., `BrokerServer`, `UnifiedLog`) |
| `kafka.client.id` | string | Client ID (producer, consumer, admin) |
| `kafka.client.type` | string | One of: `producer`, `consumer`, `admin` |
| `kafka.group.id` | string | Consumer/share group ID |
| `kafka.group.instance.id` | string | Static group membership instance ID |
| `kafka.transactional.id` | string | Transactional producer ID |
| `kafka.topic` | string | Topic name |
| `kafka.partition` | string | Partition number |
| `kafka.connector` | string | Kafka Connect connector name |
| `kafka.task.id` | string | Kafka Connect task ID |
| `kafka.controller.id` | string | KRaft active controller ID |

All keys use the `kafka.` prefix to avoid collision with user-defined MDC entries.
Keys whose MDC value is null are **omitted** from the JSON output (not emitted as
`"kafka.topic": null`).

### 3. JSON Log Schema (`log4j2-json-template.json`)

The JSON template file defines the output schema for structured log events. Every JSON
log line contains these fixed fields:

| Field | Source | Description |
|---|---|---|
| `timestamp` | Log4j2 event | ISO-8601 timestamp in UTC |
| `level` | Log4j2 event | Log level name (`INFO`, `WARN`, `ERROR`, etc.) |
| `logger` | Log4j2 event | Logger name (fully qualified class name) |
| `message` | Log4j2 event | The formatted log message (includes the `LogContext` prefix) |
| `thread` | Log4j2 event | Thread name |
| `exception` | Log4j2 event | Stack trace string (omitted when no exception) |
| `kafka.*` | MDC | All standard MDC keys from the table above (omitted when null) |

This template file is a configuration artifact, not compiled code. Users can customize
it by providing their own template via the `eventTemplateUri` property in the Log4j2
YAML config.

### 4. New Runtime Dependency

| Artifact | Version | Scope |
|---|---|---|
| `org.apache.logging.log4j:log4j-layout-template-json` | Same as `log4j-core` (currently 2.25.4) | Runtime (only needed when JSON config is active) |

This artifact has no transitive dependencies beyond `log4j-core` (already a Kafka
dependency). It is added to the `log4j2Libs` and `log4jReleaseLibs` dependency groups.

### 5. No Wire Protocol Changes

This KIP introduces **no changes** to the Kafka wire protocol, configuration properties,
metrics, or JMX MBeans. The changes are purely in the logging infrastructure layer.

## Proposed Changes

This KIP introduces **opt-in structured JSON logging** for all Kafka components (broker,
controller, clients, Connect, Streams, and CLI tools) with zero impact on the default
text logging behavior.

### 1. LogContext MDC Integration

The `LogContext` class (the central logging abstraction used by all Kafka Java
components) is enhanced to accept a **structured context map** alongside the existing
string prefix:

```java
// Before (still works - fully backward compatible)
new LogContext("[BrokerServer id=0] ")

// After (new overload with structured context)
new LogContext(
    "[BrokerServer id=0] ",
    Map.of("kafka.node.id", "0", "kafka.component", "BrokerServer"))
```

The context map entries are pushed to the SLF4J MDC before each log call and removed
after. This makes them available as first-class fields to any SLF4J-compatible
structured logging layout (Log4j2 `JsonTemplateLayout`, Logback `JsonEncoder`, etc.).

**Key design decisions:**

- **Per-call MDC push/pop**: Context is pushed to MDC only for the duration of each log
  call, then cleaned up. This avoids cross-contamination between components that share
  threads, and is consistent with the existing prefix-per-call pattern.

- **Zero overhead when unused**: When `contextMap` is empty (the default for the
  existing single-argument constructor), no MDC operations occur. The performance
  characteristics of existing code paths are identical.

- **Backward compatible**: The existing `LogContext(String)` and `LogContext()`
  constructors are unchanged. All existing callers continue to work with zero
  modifications.

### 2. Standard Context Field Names

A set of standard MDC key names is defined for consistent structured output:

| MDC Key | Description | Set by |
|---|---|---|
| `kafka.node.id` | Broker or controller node ID | BrokerServer, ControllerServer, SharedServer |
| `kafka.cluster.id` | Kafka cluster ID | Set after cluster metadata is available |
| `kafka.component` | Component name (e.g., `BrokerServer`, `ReplicaManager`) | Each component |
| `kafka.client.id` | Client ID | KafkaProducer, KafkaConsumer, KafkaAdminClient |
| `kafka.client.type` | Client type: `producer`, `consumer`, `admin` | Client constructors |
| `kafka.group.id` | Consumer group ID | KafkaConsumer, ShareConsumer |
| `kafka.group.instance.id` | Static group membership ID | KafkaConsumer |
| `kafka.transactional.id` | Transactional producer ID | KafkaProducer |
| `kafka.topic` | Topic name | UnifiedLog, per-partition components |
| `kafka.partition` | Partition number | UnifiedLog, per-partition components |
| `kafka.connector` | Kafka Connect connector name | Connect workers |
| `kafka.task.id` | Kafka Connect task ID | Connect tasks |
| `kafka.controller.id` | KRaft active controller ID | Controller components |

All key names use the `kafka.` prefix to avoid collision with user-defined MDC entries.

### 3. JSON Log4j2 Configuration

Three new Log4j2 configuration files are provided alongside the existing text configs:

| File | Purpose | Replaces |
|---|---|---|
| `config/log4j2-json.yaml` | Broker/Controller JSON logging | `config/log4j2.yaml` |
| `config/connect-log4j2-json.yaml` | Connect worker JSON logging | `config/connect-log4j2.yaml` |
| `config/tools-log4j2-json.yaml` | CLI tools JSON logging | `config/tools-log4j2.yaml` |

These configs use Log4j2's `JsonTemplateLayout` with a shared template file
(`config/log4j2-json-template.json`) that defines the output schema.

**Example JSON output:**
```json
{
  "timestamp": "2026-05-12T10:00:00.000+0000",
  "level": "INFO",
  "logger": "kafka.server.BrokerServer",
  "message": "[BrokerServer id=0] Started broker server",
  "thread": "main",
  "kafka.node.id": "0",
  "kafka.component": "BrokerServer"
}
```

```json
{
  "timestamp": "2026-05-12T10:00:01.234+0000",
  "level": "WARN",
  "logger": "o.a.k.storage.internals.log.UnifiedLog",
  "message": "[UnifiedLog partition=payments-3, dir=/data/kafka] Non-monotonic update of high watermark from 1000 to 999",
  "thread": "data-plane-kafka-request-handler-0",
  "kafka.topic": "payments",
  "kafka.partition": "3",
  "kafka.component": "UnifiedLog"
}
```

Fields with null MDC values are omitted from the JSON output (the `JsonTemplateLayout`
default behavior), keeping messages compact.

### 4. Enabling JSON Logging

Users switch to JSON logging by setting the Log4j2 configuration system property:

```bash
# Broker/Controller
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:config/log4j2-json.yaml"

# Connect
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:config/connect-log4j2-json.yaml"

# CLI Tools
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:config/tools-log4j2-json.yaml"
```

No code changes, restarts with the new config, or broker configuration changes are
required. The default `log4j2.yaml` continues to produce text output.

### 5. New Dependency

The `log4j-layout-template-json` artifact (same version as the existing `log4j-core`
dependency) is added to provide `JsonTemplateLayout` support. This is a Log4j2 module
with no transitive dependencies beyond `log4j-core`.

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

- **Fully backward compatible**. The default log format is unchanged.
- The existing `LogContext(String)` and `LogContext()` constructors are unchanged.
- All existing log4j2 configuration files are unchanged.
- JSON logging is strictly opt-in via config file selection.

### Migration Path

1. **Phase 1 (this KIP)**: Add MDC infrastructure, JSON config files, and structured
   context to key components (BrokerServer, ControllerServer, KafkaProducer,
   KafkaConsumer, KafkaAdminClient, UnifiedLog).

2. **Phase 2 (follow-up)**: Extend structured context to remaining components
   (ReplicaManager, KafkaApis, Partition, ReplicaFetcher, GroupCoordinator,
   TransactionCoordinator, etc.).

3. **Phase 3 (follow-up)**: Migrate the Scala `Logging` trait with `logIdent` to also
   populate MDC, covering legacy Scala server code.

## Rejected Alternatives

### 1. Replace LogContext prefix with MDC-only

Removing the string prefix would break the text log format for all users. The prefix
remains for backward-compatible text output, while MDC provides the structured
equivalent.

### 2. Use Log4j2 API directly instead of SLF4J MDC

Using Log4j2's `ThreadContext` or `MapMessage` directly would couple Kafka to Log4j2.
SLF4J MDC is the portable standard and works with any SLF4J backend (Logback, etc.).

### 3. Logback JsonEncoder instead of Log4j2 JsonTemplateLayout

Kafka uses Log4j2 as its logging implementation, not Logback. The `JsonTemplateLayout`
is the Log4j2-native solution, performs well, and is highly configurable via the
template file.

### 4. Always output JSON (no opt-in)

Forcing JSON output would break existing log parsing pipelines, monitoring, and
operational runbooks. Opt-in via config file swap is the safest migration path.

### 5. Static MDC (set once per thread) instead of per-call push/pop

Many Kafka components share threads (e.g., request handler threads process requests
for different topics/partitions). Static MDC would cause cross-contamination of context
fields. Per-call push/pop is the only correct approach for Kafka's threading model.

## Test Plan

- Unit tests for `LogContext` verifying:
  - MDC is populated during log calls and cleaned up after
  - Empty context map results in zero MDC operations
  - Existing MDC entries are not disturbed
  - Context map immutability
  - Backward compatibility with prefix-only constructors
- Integration test: start a broker with `log4j2-json.yaml`, verify log output is valid
  JSON with expected fields
- Performance test: benchmark log throughput with empty vs. populated context map to
  confirm negligible overhead

## Appendix: Files Changed

| File | Change |
|---|---|
| `clients/.../LogContext.java` | Add `contextMap` field, MDC push/pop in log wrappers |
| `gradle/dependencies.gradle` | Add `log4j-layout-template-json` dependency |
| `build.gradle` | Wire new dependency into `log4j2Libs` and `log4jReleaseLibs` |
| `config/log4j2-json-template.json` | JSON template defining output schema |
| `config/log4j2-json.yaml` | Server JSON logging config |
| `config/connect-log4j2-json.yaml` | Connect JSON logging config |
| `config/tools-log4j2-json.yaml` | Tools JSON logging config |
| `core/.../BrokerServer.scala` | Pass structured context map |
| `core/.../ControllerServer.scala` | Pass structured context map |
| `core/.../SharedServer.scala` | Pass structured context map |
| `clients/.../KafkaProducer.java` | Pass structured context map |
| `clients/.../ConsumerUtils.java` | Pass structured context map |
| `clients/.../KafkaAdminClient.java` | Pass structured context map |
| `server/.../BrokerLifecycleManager.java` | Pass structured context map |
| `server/.../AssignmentsManager.java` | Pass structured context map |
| `server/.../ControllerRegistrationManager.java` | Pass structured context map |
| `storage/.../UnifiedLog.java` | Pass structured context map with topic/partition |
