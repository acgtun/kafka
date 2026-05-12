# KIP-XXXX: Structured MDC Context for Kafka Logging

## Status

**Current State**: Draft

**Author(s)**: Haifeng Chen

**Created**: 2026-05-12

**Target Kafka Version**: 4.2

**Discussion Thread**: (link)

**JIRA**: (link)

**Released**: N/A

## Motivation

KIP-653 (Kafka 4.0) upgraded Kafka from Log4j 1.x to Log4j 2.x, which supports
structured JSON output via `JsonTemplateLayout`. Operators can already switch to JSON
logging by editing `log4j2.yaml`. However, doing so today produces logs with **empty
context fields** because Kafka's broker, controller, and client components do not
populate the SLF4J Mapped Diagnostic Context (MDC):

```json
{
  "timestamp": "2026-05-12T10:00:00.000+0000",
  "level": "WARN",
  "logger": "org.apache.kafka.storage.internals.log.UnifiedLog",
  "message": "[UnifiedLog partition=payments-3, dir=/data/kafka] Non-monotonic update of high watermark",
  "thread": "data-plane-kafka-request-handler-0"
}
```

The broker ID, topic, and partition are buried inside the message string prefix
(`[UnifiedLog partition=payments-3, dir=/data/kafka]`). There are no structured fields
a log aggregation system can index, filter, or alert on without custom regex parsing.

KIP-449 (Kafka 2.3) proved this problem is solvable: it added MDC context to Kafka
Connect, populating `connector.context` with the connector name and task ID. This
enables `%X{connector.context}` in log patterns and structured fields in JSON output.
But this approach was never extended to the broker, controller, or client libraries.

**This KIP closes that gap.** It populates SLF4J MDC with standardized context fields
(`kafka.node.id`, `kafka.topic`, `kafka.partition`, `kafka.client.id`, etc.) across
all major Kafka components, and ships ready-to-use JSON Log4j2 configuration files.

After this KIP, the same log event produces:

```json
{
  "timestamp": "2026-05-12T10:00:00.000+0000",
  "level": "WARN",
  "logger": "org.apache.kafka.storage.internals.log.UnifiedLog",
  "message": "[UnifiedLog partition=payments-3, dir=/data/kafka] Non-monotonic update of high watermark",
  "thread": "data-plane-kafka-request-handler-0",
  "kafka.topic": "payments",
  "kafka.partition": "3",
  "kafka.node.id": "0",
  "kafka.component": "UnifiedLog"
}
```

### The Problem in Detail

1. **MDC is empty**: The broker, controller, and all client libraries (producer,
   consumer, admin) do not set any MDC fields. Only Kafka Connect (via KIP-449) sets
   `connector.context`. This means `JsonTemplateLayout` MDC resolvers produce null for
   every field on every log line.

2. **Context is trapped in string prefixes**: The `LogContext` class prepends a
   human-readable prefix like `[BrokerServer id=0]` or `[Producer clientId=p1]` to
   every log message. This context is useful for text logs but is not available as
   structured fields for log aggregation, filtering, or alerting.

3. **No standard field names**: Even if operators manually called `MDC.put()` in their
   code, there is no agreed-upon naming convention for context fields across Kafka
   components. Each deployment would invent its own names.

4. **Cross-component correlation is impossible**: There is no shared field (like
   `kafka.node.id`) that appears consistently across all log messages from a given
   broker, making it hard to filter logs by source in multi-tenant or multi-cluster
   environments.

### Prior Art

| KIP | Kafka Version | What it did | Relationship to this KIP |
|---|---|---|---|
| **KIP-653** | 4.0 | Upgraded to Log4j 2.x; enabled `JsonTemplateLayout` | Provides the JSON output capability. This KIP populates the MDC fields that JSON output needs. |
| **KIP-449** | 2.3 | Added MDC context (`connector.context`) to Kafka Connect | Proved the MDC pattern works in Kafka. This KIP extends the same approach to broker, controller, and clients. |
| **KIP-673** | 2.8 | Made request/response DEBUG traces emit proper JSON | Different scope (request traces only). Complementary to this KIP. |
| **KIP-714** | 3.6 | Client metrics telemetry via OpenTelemetry Protocol | KIP-714 structures **metrics**; this KIP structures **logs**. Together they complete the Kafka observability story. |
| **KIP-916** | Under Discussion | Adds `flow.context` MDC key for MirrorMaker 2 | Same MDC pattern, narrow scope (MM2 only). Complementary to this KIP. |

### Industry Context

Structured logging with pre-populated context fields is industry standard:

- **Kubernetes**: JSON logs with structured context are the default since KEP-1602
  (GA in Kubernetes 1.24).
- **Elasticsearch**: Structured JSON logging with node/index context since version 7.
- **PostgreSQL**: `log_destination = 'jsonlog'` with structured fields since version 15.
- **MongoDB**: Structured JSON logging with component context since version 4.4.

## Public Interfaces

### 1. `LogContext` --- New Constructor and Method

`org.apache.kafka.common.utils.internals.LogContext` gains a new public constructor
and accessor:

```java
/**
 * Create a LogContext with both a human-readable prefix and a structured context map.
 *
 * @param logPrefix   the string prefix prepended to every log message (may be null)
 * @param contextMap  structured key-value pairs pushed to SLF4J MDC on each log call
 *                    (must not be null; may be empty)
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
These key names become a **stable public contract** --- log consumers (dashboards, alerts,
analysis tools) will depend on them.

| MDC Key | Type | Description | Phase |
|---|---|---|---|
| `kafka.node.id` | string | Broker or controller node ID | 1 |
| `kafka.component` | string | Component name (e.g., `BrokerServer`, `UnifiedLog`) | 1 |
| `kafka.client.id` | string | Client ID (producer, consumer, admin) | 1 |
| `kafka.client.type` | string | One of: `producer`, `consumer`, `admin` | 1 |
| `kafka.group.id` | string | Consumer/share group ID | 1 |
| `kafka.group.instance.id` | string | Static group membership instance ID | 1 |
| `kafka.transactional.id` | string | Transactional producer ID | 1 |
| `kafka.topic` | string | Topic name | 1 |
| `kafka.partition` | string | Partition number | 1 |
| `kafka.cluster.id` | string | Kafka cluster ID | 2 (reserved) |
| `kafka.connector` | string | Kafka Connect connector name | 2 (reserved) |
| `kafka.task.id` | string | Kafka Connect task ID | 2 (reserved) |

All keys use the `kafka.` prefix to avoid collision with user-defined MDC entries.
Keys whose MDC value is null are **omitted** from the JSON output (not emitted as
`"kafka.topic": null`).

Phase 2 keys are reserved in the JSON template for forward compatibility but are not
populated by any code in this KIP. They will be implemented in follow-up work.

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

**Note on message prefix duplication:** The `message` field includes the `LogContext`
string prefix (e.g., `[BrokerServer id=0]`) for backward compatibility with text-mode
parsing tools and for human readability. The same information is available as structured
MDC fields (e.g., `kafka.node.id`). Users who want a cleaner `message` field can provide
a custom `LogContext` subclass or post-process the JSON output. Removing the prefix from
JSON-mode messages may be addressed in a future KIP.

### 4. New Runtime Dependency

| Artifact | Version | Scope |
|---|---|---|
| `org.apache.logging.log4j:log4j-layout-template-json` | Same as `log4j-core` (currently 2.25.4) | Runtime (only needed when JSON config is active) |

This artifact is ~80KB with no transitive dependencies beyond `log4j-core` (already a
Kafka dependency). It is added to the `log4j2Libs` and `log4jReleaseLibs` dependency
groups. The minimum Log4j2 version required for `JsonTemplateLayout` is 2.14.0; Kafka
currently ships 2.25.4.

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
// Before (still works --- fully backward compatible)
new LogContext("[BrokerServer id=0] ")

// After (new overload with structured context)
new LogContext(
    "[BrokerServer id=0] ",
    Map.of("kafka.node.id", "0", "kafka.component", "BrokerServer"))
```

The context map entries are pushed to the SLF4J MDC before each log call and restored
after. Any pre-existing MDC values for the same keys are saved before the push and
restored after, ensuring that other components' MDC entries (e.g., Kafka Connect's
`LoggingContext` which sets `connector.context`) are never clobbered.

**Key design decisions:**

- **Per-call MDC save/push/restore**: Context is pushed to MDC only for the duration
  of each log call. Pre-existing MDC values are saved before the push and restored
  after. This avoids cross-contamination between components that share threads and
  prevents clobbering of MDC values set by other frameworks (e.g., Kafka Connect's
  `LoggingContext`). The save state is a method-local variable, so nested/reentrant
  log calls each get their own independent save state.

- **Level guards on all log methods**: All log methods (trace, debug, info, warn, error)
  check the log level before performing any work. This ensures zero overhead (no MDC
  operations, no message formatting, no object allocation) when the level is disabled.

- **Zero overhead when unused**: When `contextMap` is empty (the default for the
  existing single-argument constructor), no MDC operations occur. The performance
  characteristics of existing code paths are identical.

- **Backward compatible**: The existing `LogContext(String)` and `LogContext()`
  constructors are unchanged. All existing callers continue to work with zero
  modifications.

### 2. Standard Context Field Names

A set of standard MDC key names is defined for consistent structured output:

| MDC Key | Description | Set by | Phase |
|---|---|---|---|
| `kafka.node.id` | Broker or controller node ID | BrokerServer, ControllerServer, SharedServer | 1 |
| `kafka.component` | Component name | Each component | 1 |
| `kafka.client.id` | Client ID | KafkaProducer, KafkaConsumer, KafkaAdminClient | 1 |
| `kafka.client.type` | `producer`, `consumer`, or `admin` | Client constructors | 1 |
| `kafka.group.id` | Consumer group ID | KafkaConsumer, ShareConsumer | 1 |
| `kafka.group.instance.id` | Static group membership ID | KafkaConsumer | 1 |
| `kafka.transactional.id` | Transactional producer ID | KafkaProducer | 1 |
| `kafka.topic` | Topic name | UnifiedLog | 1 |
| `kafka.partition` | Partition number | UnifiedLog | 1 |
| `kafka.cluster.id` | Kafka cluster ID | (Phase 2) | 2 |
| `kafka.connector` | Connect connector name | (Phase 2) | 2 |
| `kafka.task.id` | Connect task ID | (Phase 2) | 2 |

All key names use the `kafka.` prefix to avoid collision with user-defined MDC entries.

The standardized key names are a key differentiator from "just configure MDC yourself":
users get pre-populated, consistently named context fields across all Kafka components
without any instrumentation effort. The `LogContext` wrapper also handles MDC
save/restore correctly for Kafka's shared-thread model, which is non-trivial to
implement correctly in user code.

### 3. Interaction with Kafka Connect's Existing MDC Usage

Kafka Connect already uses SLF4J MDC via its `LoggingContext` class, which sets
`connector.context` in MDC. This KIP's `kafka.*` keys do not conflict with
`connector.context`. Additionally, the save/restore pattern in `LogContext` ensures
that any pre-existing MDC entries (including Connect's) are preserved across log calls.

In Phase 2, Connect workers will be updated to set `kafka.connector` and `kafka.task.id`
via `LogContext` context maps, providing structured fields alongside the existing
`connector.context` prefix.

### 4. JSON Log4j2 Configuration

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
  "logger": "org.apache.kafka.storage.internals.log.UnifiedLog",
  "message": "[UnifiedLog partition=payments-3, dir=/data/kafka] Non-monotonic update of high watermark from 1000 to 999",
  "thread": "data-plane-kafka-request-handler-0",
  "kafka.topic": "payments",
  "kafka.partition": "3",
  "kafka.component": "UnifiedLog"
}
```

Fields with null MDC values are omitted from the JSON output (the `JsonTemplateLayout`
default behavior), keeping messages compact.

### 5. Enabling JSON Logging

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

**Containerized deployments:** In Docker/Kubernetes environments, set `KAFKA_LOG4J_OPTS`
in the container environment (e.g., Helm `values.yaml`, `docker-compose.yml`, or
Kubernetes `ConfigMap`). Strimzi users can configure this via `spec.kafka.logging` in
the Kafka CRD.

### 6. New Dependency

The `log4j-layout-template-json` artifact (same version as the existing `log4j-core`
dependency, ~80KB) is added to provide `JsonTemplateLayout` support. This is a Log4j2
module with no transitive dependencies beyond `log4j-core`.

### 7. Phase 1 Coverage

Phase 1 instruments the following components with structured context maps:

**Server-side (6 components):**
- `BrokerServer`, `ControllerServer`, `SharedServer` --- `kafka.node.id`, `kafka.component`
- `BrokerLifecycleManager`, `AssignmentsManager`, `ControllerRegistrationManager` --- `kafka.node.id`, `kafka.component`

**Client-side (3 components):**
- `KafkaProducer` --- `kafka.client.id`, `kafka.client.type`, `kafka.transactional.id`
- `KafkaConsumer` (via `ConsumerUtils`) --- `kafka.client.id`, `kafka.client.type`, `kafka.group.id`, `kafka.group.instance.id`
- `KafkaAdminClient` --- `kafka.client.id`, `kafka.client.type`

**Storage (1 component):**
- `UnifiedLog` --- `kafka.topic`, `kafka.partition`, `kafka.component`

**Scala `Logging` trait partial coverage:** The Scala `Logging` trait (used by ~100+
classes like `ReplicaManager`, `KafkaApis`, `Partition`) does not go through
`LogContext` and is not instrumented in Phase 1. As a partial mitigation,
`BrokerServer` and `ControllerServer` set `kafka.node.id` in the thread-level MDC at
startup, so Scala loggers on the main broker/controller threads inherit this basic
context. Full coverage of the Scala `Logging` trait is Phase 3.

## Performance Impact

The per-call MDC save/push/restore pattern adds overhead only when `contextMap` is
non-empty AND the log level is enabled:

**When `contextMap` is empty (default):** Zero overhead. The `hasContext` flag
short-circuits all MDC operations. Existing code paths are unaffected.

**When `contextMap` is non-empty and level is disabled:** Zero overhead. Level guards
(`isInfoEnabled()`, `isWarnEnabled()`, etc.) prevent any work.

**When `contextMap` is non-empty and level is enabled:** Each log call performs:
- 1 `HashMap` allocation (sized to contextMap, short-lived young-gen object)
- N `MDC.get()` calls (save prior values)
- N `MDC.put()` calls (push context)
- N `MDC.put()` or `MDC.remove()` calls (restore)

Where N is the number of context map entries (typically 2-3).

This overhead is negligible compared to the cost of the log call itself (message
formatting, string concatenation, I/O to log file, potential disk flush). MDC
operations are `ThreadLocal` HashMap lookups --- each is ~10-20ns. For a typical
context of 3 entries, the total overhead is ~100-200ns per log call, compared to
~1-10us for message formatting and I/O.

The `HashMap` allocation is in Java's young generation and collected immediately. On
high-throughput brokers logging at INFO level, the additional allocation rate is
bounded by the INFO/WARN/ERROR log rate (typically hundreds to low thousands per
second), not the message processing rate (millions per second).

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

- **Fully backward compatible**. The default log format is unchanged.
- The existing `LogContext(String)` and `LogContext()` constructors are unchanged.
- All existing log4j2 configuration files are unchanged.
- JSON logging is strictly opt-in via config file selection.

### Migration Path

1. **Phase 1 (this KIP)**: Add MDC infrastructure, JSON config files, and structured
   context to key components (BrokerServer, ControllerServer, KafkaProducer,
   KafkaConsumer, KafkaAdminClient, UnifiedLog). See "Phase 1 Coverage" above for
   the complete list.

2. **Phase 2 (follow-up)**: Extend structured context to remaining components
   (ReplicaManager, KafkaApis, Partition, ReplicaFetcher, GroupCoordinator,
   TransactionCoordinator, Kafka Connect workers, etc.). Populate `kafka.cluster.id`,
   `kafka.connector`, and `kafka.task.id`.

3. **Phase 3 (follow-up)**: Migrate the Scala `Logging` trait with `logIdent` to also
   populate MDC, covering legacy Scala server code.

### Known Limitations

- **Async logging**: Log4j2's default `AsyncAppender` snapshots MDC at enqueue time,
  so structured context is correctly preserved in async mode. Custom async
  implementations that do not snapshot MDC at enqueue time may lose context fields.
  This is documented in the JSON YAML config files.

- **Virtual threads (Project Loom)**: SLF4J 1.7.x MDC uses `ThreadLocal`, which binds
  to the carrier thread, not the virtual thread. If Kafka adopts virtual threads in
  the future, MDC-based structured logging will require SLF4J 2.x (which uses
  `ScopedValue`) or Log4j2 2.24+ for correct context propagation. This is not a
  concern today since Kafka does not use virtual threads internally.

- **Scala `Logging` trait coverage**: See "Phase 1 Coverage" above.

- **Message prefix duplication**: See "Note on message prefix duplication" in the
  Public Interfaces section.

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

### 5. Static MDC (set once per thread) instead of per-call save/push/restore

Many Kafka components share threads (e.g., request handler threads process requests
for different topics/partitions). Static MDC would cause cross-contamination of context
fields between components. Per-call save/push/restore is the only correct approach for
Kafka's threading model. The save/restore step ensures that pre-existing MDC values
(from other frameworks like Kafka Connect's `LoggingContext`) are never clobbered.

### 6. Broker configuration property (e.g., `log.format=json`)

Log4j2 configuration is handled via the logging framework's own configuration system,
not Kafka broker configuration. Adding a broker config would create a second, redundant
mechanism for configuring the logging format and would not cover client-side or CLI
tool logging. The environment variable approach (`KAFKA_LOG4J_OPTS`) is the established
pattern for Kafka logging configuration and works consistently across all components.

### 7. "KIP-653 already enables JSON logging --- this KIP is unnecessary"

KIP-653 (Log4j 2 upgrade) provides the **output mechanism** (`JsonTemplateLayout`), but
not the **input data**. Switching to `JsonTemplateLayout` today produces JSON with empty
context fields because no Kafka component populates MDC. This KIP populates MDC with
structured context so that JSON output actually contains useful data. The relationship
is analogous to providing a database schema (KIP-653) vs. populating the database with
data (this KIP).

### 8. "Users can configure MDC themselves"

Users can call `MDC.put()` manually and use `%X{key}` in Log4j2 patterns. However,
this KIP provides value beyond what users can do themselves:
- **Standardized key names** across all Kafka components --- users would need to agree
  on and enforce naming conventions across their organization
- **Pre-populated context** --- users cannot instrument Kafka's internal components
  (e.g., `UnifiedLog`, `BrokerLifecycleManager`) without modifying Kafka source code
- **Correct save/restore** --- Kafka's shared-thread model (e.g., request handler
  threads serving multiple topics/partitions) requires per-call MDC save/restore to
  avoid cross-contamination. This is non-trivial to implement correctly in user code.
  KIP-449 solved this for Connect; this KIP solves it for the rest of Kafka.
- **Official JSON template** --- a ready-to-use config file with the right field schema

## Test Plan

### Implemented (in this KIP):

- Unit tests for `LogContext` (`LogContextTest`, 18 tests) verifying:
  - MDC is populated during log calls and cleaned up after
  - Pre-existing MDC values are saved and restored (not clobbered)
  - Empty context map results in zero MDC operations
  - Existing MDC entries from other sources are not disturbed
  - Context map immutability
  - Null contextMap parameter rejected with NullPointerException
  - Backward compatibility with prefix-only constructors
  - Reentrancy safety (nested log calls don't corrupt MDC state)
  - Concurrent thread safety (10 threads, 100 iterations each)

### Planned (follow-up):

- Integration test: start a broker with `log4j2-json.yaml`, verify log output is valid
  JSON with expected fields
- Performance test: JMH benchmark in `jmh-benchmarks/` comparing log throughput with
  empty vs. populated context map

## Appendix: Files Changed

| File | Change |
|---|---|
| `clients/.../LogContext.java` | Add `contextMap` field, MDC save/push/restore in log wrappers, level guards on all methods |
| `clients/.../LogContextTest.java` | 18 unit tests for MDC behavior |
| `gradle/dependencies.gradle` | Add `log4j-layout-template-json` dependency |
| `build.gradle` | Wire new dependency into `log4j2Libs` and `log4jReleaseLibs` |
| `config/log4j2-json-template.json` | JSON template defining output schema |
| `config/log4j2-json.yaml` | Server JSON logging config |
| `config/connect-log4j2-json.yaml` | Connect JSON logging config |
| `config/tools-log4j2-json.yaml` | Tools JSON logging config |
| `core/.../BrokerServer.scala` | Pass structured context map; set thread-level MDC |
| `core/.../ControllerServer.scala` | Pass structured context map; set thread-level MDC |
| `core/.../SharedServer.scala` | Pass structured context map |
| `clients/.../KafkaProducer.java` | Pass structured context map |
| `clients/.../ConsumerUtils.java` | Pass structured context map |
| `clients/.../KafkaAdminClient.java` | Pass structured context map |
| `server/.../BrokerLifecycleManager.java` | Pass structured context map |
| `server/.../AssignmentsManager.java` | Pass structured context map |
| `server/.../ControllerRegistrationManager.java` | Pass structured context map |
| `storage/.../UnifiedLog.java` | Pass structured context map with topic/partition |
