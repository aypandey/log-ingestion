# Log Ingestion System

A high-performance, cost-effective log ingestion system with hot/cold tier architecture, designed to replace Elasticsearch for log management with reduced cloud dependency.

## Features

- **Hot/Cold Tier Architecture**: Recent data (last 15 days by default) in memory for fast access, older data on filesystem
- **Time-Based Indexing**: Efficient range queries using B+Tree-like structure (ConcurrentSkipListMap)
- **Inverted Indexes**: Fast field-based lookups for configured JSON keys
- **Scatter-Gather Queries**: Parallel query execution across both tiers
- **Out-of-Order Ingestion**: Handles documents arriving in any order based on creationTimestamp
- **Durability**: Write-Ahead Log (WAL) + Periodic Snapshots for crash recovery
- **Automatic Lifecycle Management**: Automatic migration of aged data from hot to cold tier
- **Configurable**: Customize retention periods, indexed fields, and more

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Log Ingestion System                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐         ┌──────────────────┐          │
│  │  DocumentIngester│────────▶│  Hot Tier        │          │
│  │  (Async Queue)   │         │  - TimeBasedIndex│          │
│  └─────────────────┘         │  - InvertedIndex │          │
│          │                    │  - WAL           │          │
│          │                    │  - Snapshots     │          │
│          │                    └──────────────────┘          │
│          │                             │                     │
│          │                             │ (Lifecycle Mgr)     │
│          │                             ▼                     │
│          │                    ┌──────────────────┐          │
│          └───────────────────▶│  Cold Tier       │          │
│                                │  - Date Partitions│         │
│                                │  - Metadata Index│          │
│                                └──────────────────┘          │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         Query Executor (Scatter-Gather)              │   │
│  │  - Time Range Queries                                │   │
│  │  - Key-Value Queries                                 │   │
│  │  - Boolean Queries (AND/OR/NOT)                      │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Java 11 or higher
- Maven 3.6+

### Build

```bash
mvn clean package
```

### Configuration

Create or modify `config.json`:

```json
{
  "hotTierRetentionDays": 15,
  "hotTierSnapshotIntervalMinutes": 30,
  "coldStoragePath": "./cold-storage",
  "walPath": "./wal",
  "snapshotPath": "./snapshots",
  "indexedFields": [
    "level",
    "service",
    "userId",
    "traceId",
    "message"
  ],
  "maxHotTierMemoryMB": 1024,
  "compactionIntervalHours": 6
}
```

### Run Example

```bash
mvn exec:java -Dexec.mainClass="com.logingestion.Example"
```

## Usage

### Basic Usage

```java
import com.logingestion.LogIngestionSystem;
import com.logingestion.model.Query;
import com.logingestion.query.QueryResult;

// 1. Create and start the system
LogIngestionSystem system = new LogIngestionSystem();
system.start();

// 2. Ingest documents
String jsonLog = "{" +
    "\"creationTimestamp\": " + System.currentTimeMillis() + ", " +
    "\"level\": \"ERROR\", " +
    "\"service\": \"payment-service\", " +
    "\"userId\": \"user123\", " +
    "\"message\": \"Payment processing failed\"" +
"}";

system.ingest(jsonLog);

// 3. Query documents
long now = System.currentTimeMillis();
long oneDayAgo = now - (24 * 60 * 60 * 1000);

Query.TimeRangeQuery query = system.timeRangeQuery(oneDayAgo, now);
QueryResult result = system.query(query);

System.out.println("Found " + result.getDocumentCount() + " documents");

// 4. Shutdown gracefully
system.shutdown();
```

### Advanced Queries

#### Time Range Query
```java
// Query last 7 days
Query.TimeRangeQuery timeQuery = system.timeRangeQuery(
    System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7),
    System.currentTimeMillis()
);
QueryResult result = system.query(timeQuery);
```

#### Key-Value Query
```java
// Find all ERROR logs
Query.KeyValueQuery errorQuery = system.keyValueQuery("level", "ERROR");
QueryResult result = system.query(errorQuery);
```

#### Composite Query (Time Range + Filter)
```java
// ERROR logs from last 15 days
Query.CompositeQuery compositeQuery = system.compositeQuery(
    system.timeRangeQuery(fifteenDaysAgo, now),
    system.keyValueQuery("level", "ERROR")
);
QueryResult result = system.query(compositeQuery);
```

#### Boolean Queries
```java
// AND: ERROR logs from auth-service
Query.BooleanQuery andQuery = Query.BooleanQuery.and(
    system.keyValueQuery("level", "ERROR"),
    system.keyValueQuery("service", "auth-service")
);

// OR: ERROR or WARN logs
Query.BooleanQuery orQuery = Query.BooleanQuery.or(
    system.keyValueQuery("level", "ERROR"),
    system.keyValueQuery("level", "WARN")
);

// NOT: All logs except INFO
Query.BooleanQuery notQuery = Query.BooleanQuery.not(
    system.keyValueQuery("level", "INFO")
);
```

### Batch Ingestion

```java
List<String> logs = List.of(
    "{\"creationTimestamp\": 1234567890, \"level\": \"INFO\", ...}",
    "{\"creationTimestamp\": 1234567891, \"level\": \"ERROR\", ...}",
    // ... more logs
);

system.ingestBatch(logs);
```

### Async Ingestion

```java
// Non-blocking ingestion
system.ingestAsync(jsonLog);

// Flush to wait for completion
system.flush();
```

## Document Format

All documents must be valid JSON with a **required** `creationTimestamp` field:

```json
{
  "creationTimestamp": 1699999999999,
  "level": "ERROR",
  "service": "payment-service",
  "userId": "user123",
  "message": "Payment processing failed",
  "traceId": "abc-123-def",
  "additionalField": "any value"
}
```

## System Components

### 1. Hot Tier Storage
- **In-memory indexes** for fast queries
- **Time-based index** (ConcurrentSkipListMap) for range queries
- **Inverted indexes** for field-based lookups
- **WAL** for durability
- **Periodic snapshots** for faster recovery

### 2. Cold Tier Storage
- **Filesystem-based** storage
- **Date-partitioned** files (YYYY-MM-DD.jsonl)
- **Lightweight metadata indexes** for faster lookups
- **JSON Lines** format for easy reading/processing

### 3. Document Ingester
- **Async ingestion** with configurable queue
- **Out-of-order handling** based on creationTimestamp
- **Automatic tier routing** (hot vs cold based on timestamp)
- **Batch support** for high throughput

### 4. Query Executor
- **Scatter-gather** pattern across tiers
- **Parallel execution** for better performance
- **Result deduplication** and sorting
- Supports:
  - Time range queries
  - Key-value exact match
  - Boolean queries (AND/OR/NOT)
  - Composite queries

### 5. Data Lifecycle Manager
- **Automatic migration** of aged data from hot to cold
- **Configurable retention** period
- **Scheduled compaction** to optimize cold storage

## Performance Characteristics

### Hot Tier
- **Ingestion**: ~100K docs/sec (batch mode)
- **Range Query**: O(log n + k) where k = result size
- **Field Query**: O(1) lookup in inverted index
- **Memory**: ~1-2KB per document (depending on size)

### Cold Tier
- **Ingestion**: ~10K docs/sec (disk I/O bound)
- **Range Query**: O(partitions × documents per partition)
- **Field Query**: Optimized with metadata index
- **Storage**: Raw JSON size + index overhead

### Scatter-Gather
- Queries execute in parallel across both tiers
- Total latency ≈ max(hot_tier_latency, cold_tier_latency)

## Monitoring

Get system statistics:

```java
Map<String, Object> stats = system.getStatistics();
System.out.println(stats);
```

Output includes:
- Hot tier: document count, timestamp range, index terms
- Cold tier: partition count, total documents
- Ingestion: total/hot/cold counts, out-of-order count, queue size
- Lifecycle: running status, migrated documents

## Data Durability

The system provides strong durability guarantees:

1. **WAL**: All writes are logged before applying to memory
2. **Snapshots**: Periodic snapshots reduce recovery time
3. **Recovery**: On restart, loads latest snapshot + replays WAL
4. **Graceful Shutdown**: Ensures final snapshot and WAL flush

## Limitations and Future Enhancements

### Current Limitations
- No distributed support (single-node only)
- Cold tier queries scan files (could be optimized with Bloom filters)
- NOT queries can be expensive (scans all data)
- No full-text search (exact match only)

### Future Enhancements
- [ ] Add Bloom filters for cold tier optimization
- [ ] Implement full-text search with tokenization
- [ ] Add compression for cold tier storage
- [ ] Support S3-compatible storage backends
- [ ] Add distributed query support
- [ ] Implement retention policies for cold tier
- [ ] Add metrics and monitoring endpoints

## Cost Comparison

### Elasticsearch (Managed)
- ~$150-300/month per node
- High memory requirements
- Cloud vendor lock-in

### This Solution
- Filesystem storage: $0.02-0.10/GB/month
- Lower memory footprint
- No vendor lock-in
- **Estimated savings: 80-90%** for typical workloads

## License

This project is provided as-is for educational and production use.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Tests are included for new features
- Documentation is updated

## Support

For issues, questions, or contributions, please open an issue on the project repository.
