## Tiered Log Storage System

This system replaces Elasticsearch with a cost-effective, scalable log storage solution that combines:

Hot Tier: In-memory storage (last 15 days) for fast queries
Cold Tier: S3/filesystem storage for historical data
Scatter-Gather: Automatic query routing across tiers

```
Architecture
┌─────────────────────────────────────────────────┐
│           Ingestion Layer (Any Order)           │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│              Hot Tier (15 days)                 │
│  ┌──────────────────┐  ┌────────────────────┐  │
│  │  B+Tree Index    │  │  Inverted Index    │  │
│  │  (ConcurrentSkip │  │  (Field→Value→Docs)│  │
│  │   ListMap)       │  │                    │  │
│  └──────────────────┘  └────────────────────┘  │
└────────────────┬────────────────────────────────┘
                 │ Eviction (> 15 days)
                 ▼
┌─────────────────────────────────────────────────┐
│           Cold Tier (S3/Filesystem)             │
│  ┌──────────────────────────────────────────┐  │
│  │  Partitioned Files (1 day chunks)        │  │
│  │  - data_TIMESTAMP.json.gz                │  │
│  │  - index_TIMESTAMP.json.gz               │  │
│  │  - metadata_TIMESTAMP.json               │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
                 ▲
                 │
┌────────────────┴────────────────────────────────┐
│        Scatter-Gather Query Engine              │
│  (Parallel queries across hot & cold tiers)     │
└─────────────────────────────────────────────────┘
```

### Key Features

**1. Out-of-Order Ingestion**
- Documents can arrive in any order
- creationTimestamp field determines range placement
- Hot tier handles all ingestion initially
- Background eviction moves old data to cold tier

**2. B+Tree-like Structure**
- Uses ConcurrentSkipListMap for O(log n) range queries
- Timestamp-based ordering
- Efficient range scans

**3. Inverted Index**
- Configurable fields for indexing
- Fast field-based lookups
- Supports multi-field queries with intersection

**4. Scatter-Gather Queries**
- Automatic tier detection based on time range
- Parallel queries across tiers
- Result merging and deduplication

**5. Cost Optimization**
- In-memory for hot data (fast but expensive)
- Compressed files for cold data (slow but cheap)
- S3 Infrequent Access storage class
- GZIP compression (typically 5-10x reduction)

**note:** This system is written on Java, so if you're really concerned about extreme low latency. I would recommend to go with C.
