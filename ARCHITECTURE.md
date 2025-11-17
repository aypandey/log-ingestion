# System Architecture

## Overview

This log ingestion system is designed to replace Elasticsearch with a custom, cost-effective solution that maintains high performance while reducing cloud dependency.

## Core Design Principles

1. **Hot/Cold Tier Separation**: Recent data (hot) in memory for speed, older data (cold) on disk for cost efficiency
2. **Time-Based Partitioning**: Efficient range queries using timestamp-based indexing
3. **Inverted Indexes**: Fast field lookups for configured JSON keys
4. **Durability First**: WAL + Snapshots ensure data safety
5. **Out-of-Order Handling**: Documents routed based on creationTimestamp, not arrival time

## Component Architecture

### 1. Storage Layer

#### Hot Tier Storage
- **Purpose**: Fast access to recent data (last 15 days by default)
- **Components**:
  - `TimeBasedIndex`: ConcurrentSkipListMap for O(log n) range queries
  - `InvertedIndex`: HashMap-based inverted index for field lookups
  - `WAL`: Append-only log for durability
  - `SnapshotManager`: Periodic snapshots for fast recovery
  - `DocumentStore`: In-memory map of document ID to Document

**Data Flow**:
```
Document → WAL → In-Memory Structures (TimeIndex + InvertedIndex + Store)
                ↓
         Periodic Snapshot
```

#### Cold Tier Storage
- **Purpose**: Cost-effective storage for older data
- **Components**:
  - Date-partitioned files (YYYY-MM-DD.jsonl)
  - Metadata indexes (per partition) for field lookups
  - JSON Lines format for easy processing

**Partition Structure**:
```
cold-storage/
├── 2024-01-15.jsonl          # Data file
├── 2024-01-15.idx            # Metadata index
├── 2024-01-16.jsonl
├── 2024-01-16.idx
└── ...
```

### 2. Indexing Structures

#### Time-Based Index (B+Tree-like)
- **Implementation**: `java.util.concurrent.ConcurrentSkipListMap`
- **Key**: creationTimestamp (long)
- **Value**: List<Document>
- **Operations**:
  - Insert: O(log n)
  - Range Query: O(log n + k) where k = results
  - Delete: O(log n)

**Why SkipList over B+Tree?**
- Similar performance characteristics (O(log n))
- Thread-safe out of the box (ConcurrentSkipListMap)
- No need for complex node splitting logic
- Better for concurrent reads/writes

#### Inverted Index
- **Structure**: Map<FieldName, Map<FieldValue, Set<DocId>>>
- **Example**:
```
{
  "level": {
    "ERROR": ["doc1", "doc5", "doc9"],
    "INFO": ["doc2", "doc3", "doc4"],
    "WARN": ["doc6", "doc7"]
  },
  "service": {
    "api-gateway": ["doc1", "doc2"],
    "auth-service": ["doc3", "doc4"],
    "payment-service": ["doc5", "doc6"]
  }
}
```

- **Lookups**: O(1) average case
- **Updates**: O(1) average case
- **Memory**: ~100 bytes per unique field value

### 3. Durability Mechanisms

#### Write-Ahead Log (WAL)
- **Format**: Text-based, one operation per line
- **Entry Format**: `OPERATION|SEQUENCE_NUMBER|DATA`
- **Example**:
```
INSERT|1|doc123|1699999999|{"level":"ERROR",...}
INSERT|2|doc124|1700000000|{"level":"INFO",...}
DELETE|3|doc123
```

**Recovery Process**:
1. Load latest snapshot (if exists)
2. Replay WAL from last sequence number
3. Rebuild in-memory structures
4. Truncate WAL after successful snapshot

#### Snapshots
- **Mechanism**: Java serialization of index structures
- **Files**:
  - `snapshot_YYYYMMDD_HHmmss_timeindex.dat`
  - `snapshot_YYYYMMDD_HHmmss_invertedindex.dat`
- **Frequency**: Configurable (default: 30 minutes)
- **Retention**: Keep only latest snapshot

**Crash Recovery Time**:
- With snapshot: ~10-30 seconds (depends on data size)
- Without snapshot: ~1-5 minutes (replays entire WAL)

### 4. Query Engine

#### Query Types

1. **TimeRangeQuery**
   - Execution: Direct lookup in TimeBasedIndex
   - Hot tier: O(log n + k)
   - Cold tier: O(partitions × docs_per_partition)

2. **KeyValueQuery**
   - Execution: Lookup in InvertedIndex → retrieve documents
   - Hot tier: O(1) index lookup + O(k) document retrieval
   - Cold tier: Metadata index → partition scan

3. **BooleanQuery**
   - AND: Intersect document sets
   - OR: Union document sets
   - NOT: All docs minus matched docs (expensive!)

4. **CompositeQuery**
   - Time range filter first (reduces search space)
   - Then apply field filters

#### Scatter-Gather Pattern

```
Query Request
      |
      v
Query Planner
      |
      +-- Determine affected tiers based on time range
      |
      v
   Scatter Phase
      |
      +--------+--------+
      |                 |
   Hot Tier         Cold Tier
   Query            Query
   (async)          (async)
      |                 |
      v                 v
   Results          Results
      |                 |
      +--------+--------+
               |
           Gather Phase
               |
               +-- Deduplicate by doc ID
               +-- Sort by timestamp
               +-- Return results
```

**Optimization**:
- Hot tier cutoff: `now - retention_period`
- If query range is entirely in hot tier, skip cold tier
- If query range is entirely before hot tier, skip hot tier
- If query spans both, execute in parallel

### 5. Data Lifecycle

#### Lifecycle Manager
- **Trigger**: Scheduled (default: every 6 hours)
- **Process**:
  1. Identify expired documents (older than retention period)
  2. Write to cold tier (batch operation)
  3. Remove from hot tier (evict)
  4. Log eviction count

**Migration Flow**:
```
Hot Tier (Day 15 boundary)
     |
     v
[Documents older than 15 days]
     |
     v
Write to Cold Tier
     |
     v
Remove from Hot Tier
     |
     v
Free up memory
```

### 6. Ingestion Pipeline

#### Document Ingester

**Ingestion Flow**:
```
JSON Document
      |
      v
Parse & Extract creationTimestamp
      |
      v
Compare with hot tier cutoff
      |
      +--------+--------+
      |                 |
  Recent?           Old?
  (Hot Tier)     (Cold Tier)
      |                 |
      v                 v
  Add to Hot       Add to Cold
  (WAL first)      (Direct write)
```

**Out-of-Order Handling**:
- Documents routed based on `creationTimestamp`, not arrival time
- Old documents directly written to cold tier
- No reordering needed - each tier handles its own range

**Async Queue**:
- `BlockingQueue<Document>` with capacity 10,000
- 4 worker threads process queue
- Backpressure: blocks on queue full

### 7. Threading Model

**Thread Pools**:
1. **Snapshot Executor**: 1 thread (scheduled)
   - Periodic snapshots every N minutes

2. **Lifecycle Executor**: 1 thread (scheduled)
   - Data migration every N hours

3. **Ingestion Executor**: 4 threads
   - Process async ingestion queue

4. **Query Executor**: 4 threads
   - Scatter-gather query execution

**Concurrency Control**:
- `CopyOnWriteArrayList` for hot tier time based indexes (lock-free)
- `ConcurrentSkipListMap` for time index (lock-free reads)
- `ConcurrentHashMap` for inverted index (lock-free reads)
- WAL writes are synchronized

## Performance Characteristics

### Memory Usage (Hot Tier)

For a document of ~500 bytes JSON:
- Document storage: ~500 bytes
- Time index entry: ~50 bytes (reference)
- Inverted index entries: ~100 bytes × indexed fields
- **Total**: ~1-2 KB per document

**Example**: 1M documents × 1.5 KB = ~1.5 GB

### Disk Usage (Cold Tier)

- Raw JSON size + ~10% overhead for metadata index
- Compressible with gzip (50-70% size reduction)
- Partition overhead: ~1KB per partition

**Example**: 100M documents × 500 bytes = 50 GB raw

### Query Performance

| Query Type | Hot Tier | Cold Tier | Notes |
|------------|----------|-----------|-------|
| Time Range | 1-10ms | 10-100ms | Depends on result size |
| Key-Value | 1-5ms | 20-200ms | With metadata index |
| Boolean AND | 2-15ms | 50-500ms | Depends on selectivity |
| Boolean OR | 5-30ms | 100-1000ms | Union operation |

### Throughput

| Operation | Throughput | Bottleneck |
|-----------|------------|------------|
| Sync Ingest | 10-50K docs/sec | WAL fsync |
| Async Ingest | 100-200K docs/sec | In-memory operations |
| Batch Ingest | 100-300K docs/sec | CPU (parsing) |
| Query (hot) | 1000-5000 qps | CPU (filtering) |
| Query (cold) | 100-1000 qps | Disk I/O |

## Scaling Considerations

### Vertical Scaling
- **Memory**: More RAM = larger hot tier = fewer cold tier queries
- **CPU**: More cores = higher ingestion throughput
- **Disk**: SSD recommended for cold tier (10x faster)

### Horizontal Scaling (Future)
- Shard by timestamp range
- Hash-based partitioning for field queries
- Distributed query coordinator

## Trade-offs

### Advantages over Elasticsearch
1. **Cost**: 80-90% cheaper (no cluster overhead, simpler storage)
2. **Simplicity**: Single-node deployment, no cluster management
3. **Control**: Full control over data format and storage
4. **Memory**: Lower memory footprint for same dataset

### Disadvantages vs Elasticsearch
1. **Features**: No full-text search, no complex aggregations
2. **Scale**: Single-node limit (vertical scaling only)
3. **Query Flexibility**: Limited to indexed fields
4. **Maturity**: New system vs battle-tested Elasticsearch

## Future Optimizations

1. **Bloom Filters**: Reduce cold tier scans by 90%
2. **Compression**: LZ4/Snappy for cold tier (50% space savings)
3. **Columnar Storage**: Parquet format for analytical queries
4. **Distributed Mode**: Shard across multiple nodes
5. **S3 Backend**: Replace filesystem with object storage
6. **Query Cache**: Cache frequently accessed results
7. **Compaction**: Merge small partitions in cold tier

## Monitoring & Observability

### Metrics to Track
- Ingestion rate (docs/sec)
- Query latency (p50, p95, p99)
- Hot tier memory usage
- Cold tier disk usage
- WAL size
- Snapshot duration
- Migration lag
- Out-of-order document rate

### Health Checks
- WAL integrity
- Snapshot age
- Disk space availability
- Memory pressure
- Queue depth
