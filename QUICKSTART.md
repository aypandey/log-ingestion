# Quick Start Guide

## 5-Minute Setup

### Step 1: Build the Project
```bash
mvn clean compile
```

### Step 2: Run the Example
```bash
./run-example.sh
# or
mvn exec:java -Dexec.mainClass="com.logingestion.Example"
```

This will:
- Start the log ingestion system
- Ingest 1000 sample documents across 20 days
- Run various query examples
- Display system statistics

### Step 3: Integrate into Your Application

```java
import com.logingestion.LogIngestionSystem;
import com.logingestion.model.Query;
import com.logingestion.query.QueryResult;

public class MyApp {
    public static void main(String[] args) throws Exception {
        // Initialize
        LogIngestionSystem system = new LogIngestionSystem();
        system.start();

        // Ingest a log
        String log = String.format(
            "{\"creationTimestamp\": %d, \"level\": \"ERROR\", " +
            "\"service\": \"my-service\", \"message\": \"Something failed\"}",
            System.currentTimeMillis()
        );
        system.ingest(log);

        // Query ERROR logs from last hour
        long now = System.currentTimeMillis();
        long oneHourAgo = now - (60 * 60 * 1000);

        Query.CompositeQuery query = system.compositeQuery(
            system.timeRangeQuery(oneHourAgo, now),
            system.keyValueQuery("level", "ERROR")
        );

        QueryResult result = system.query(query);
        System.out.println("Found " + result.getDocumentCount() + " errors");

        // Display results
        result.getDocuments().forEach(doc -> {
            System.out.println(doc.getJsonContent());
        });

        // Graceful shutdown
        system.shutdown();
    }
}
```

## Configuration

Edit `config.json` to customize:

```json
{
  "hotTierRetentionDays": 15,          // Keep 15 days in memory
  "indexedFields": [                   // Fields to index
    "level", "service", "userId"
  ],
  "coldStoragePath": "./cold-storage", // Where to store old logs
  "maxHotTierMemoryMB": 1024          // Max memory for hot tier
}
```

## Common Queries

### Time Range
```java
// Last 7 days
long now = System.currentTimeMillis();
long sevenDaysAgo = now - (7 * 24 * 60 * 60 * 1000);
Query query = system.timeRangeQuery(sevenDaysAgo, now);
```

### Filter by Field
```java
// All ERROR logs
Query query = system.keyValueQuery("level", "ERROR");

// Specific service
Query query = system.keyValueQuery("service", "payment-service");
```

### Combine Filters
```java
// ERROR logs from payment-service in last 24 hours
Query timeQuery = system.timeRangeQuery(oneDayAgo, now);
Query errorQuery = system.keyValueQuery("level", "ERROR");
Query serviceQuery = system.keyValueQuery("service", "payment-service");

Query combined = system.compositeQuery(
    timeQuery,
    Query.BooleanQuery.and(errorQuery, serviceQuery)
);
```

## Document Format

**Required**: Every document must have a `creationTimestamp` field

```json
{
  "creationTimestamp": 1699999999999,
  "level": "ERROR",
  "service": "my-service",
  "userId": "user123",
  "message": "Error message here",
  "anyOtherField": "any value"
}
```

## Performance Tips

1. **Batch Ingestion**: Use `ingestBatch()` for bulk imports
   ```java
   List<String> logs = Arrays.asList(log1, log2, log3, ...);
   system.ingestBatch(logs);
   ```

2. **Async Ingestion**: Use `ingestAsync()` for non-blocking writes
   ```java
   system.ingestAsync(logJson);
   system.flush(); // Wait for completion when needed
   ```

3. **Indexed Fields**: Only index fields you'll query on
   - Each indexed field adds ~100 bytes per unique value
   - Common fields to index: level, service, userId, traceId

4. **Time Range First**: Always add a time range to your queries
   ```java
   // Good: Limits search space
   system.compositeQuery(timeRange, fieldFilter);

   // Bad: Searches all data
   system.keyValueQuery("level", "ERROR");
   ```

## Monitoring

```java
Map<String, Object> stats = system.getStatistics();
System.out.println("Hot tier docs: " +
    ((Map)stats.get("hotTier")).get("totalDocuments"));
System.out.println("Ingestion stats: " + stats.get("ingestion"));
```

## Troubleshooting

### Out of Memory
- Reduce `hotTierRetentionDays` in config
- Reduce `maxHotTierMemoryMB`
- Force migration: `system.forceMigration()`

### Slow Queries
- Ensure queries include time range
- Check if fields are indexed
- Review query in logs (level=DEBUG)

### Recovery After Crash
System automatically recovers from:
1. Latest snapshot
2. WAL replay

Recovery time: ~10-30 seconds for typical datasets

## Next Steps

1. Read [README.md](README.md) for full documentation
2. Check [ARCHITECTURE.md](ARCHITECTURE.md) for system design
3. Explore `Example.java` for more query examples
4. Customize `config.json` for your use case

## Getting Help

- Check logs in `logs/log-ingestion-system.log`
- Enable DEBUG logging in `src/main/resources/logback.xml`
- Review system statistics with `system.getStatistics()`
