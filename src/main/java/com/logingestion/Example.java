package com.logingestion;

import com.logingestion.model.Document;
import com.logingestion.model.Query;
import com.logingestion.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Example usage of the Log Ingestion System
 */
public class Example {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) {
        try {
            logger.info("=== Log Ingestion System Example ===\n");

            // 1. Create and start the system
            logger.info("1. Creating and starting the system...");
            LogIngestionSystem system = new LogIngestionSystem();
            system.start();
            logger.info("System started successfully!\n");

            // 2. Ingest some sample documents
            logger.info("2. Ingesting sample documents...");
            ingestSampleDocuments(system);
            logger.info("Sample documents ingested\n");

            // Wait a bit for async ingestion
            Thread.sleep(1000);

            // 3. Query examples
            logger.info("3. Running query examples...\n");
            runQueryExamples(system);

            // 4. Show statistics
            logger.info("\n4. System Statistics:");
            showStatistics(system);

            // 5. Test out-of-order ingestion
            logger.info("\n5. Testing out-of-order document ingestion...");
            testOutOfOrderIngestion(system);

            // 6. Test lifecycle management
            logger.info("\n6. Testing lifecycle management...");
            testLifecycleManagement(system);

            // Wait for user to examine
            logger.info("\n\nPress Ctrl+C to shutdown the system...");
            Thread.sleep(Long.MAX_VALUE);

        } catch (InterruptedException e) {
            logger.info("Interrupted, shutting down...");
        } catch (Exception e) {
            logger.error("Error running example", e);
        }
    }

    /**
     * Ingest sample log documents
     */
    private static void ingestSampleDocuments(LogIngestionSystem system) throws Exception {
        long now = System.currentTimeMillis();
        Random random = new Random();

        List<String> services = List.of("api-gateway", "auth-service", "payment-service", "user-service");
        List<String> levels = List.of("INFO", "WARN", "ERROR", "DEBUG");
        List<String> users = List.of("user123", "user456", "user789");

        List<String> documents = new ArrayList<>();

        // Generate documents for the last 20 days
        for (int day = 0; day < 20; day++) {
            long dayTimestamp = now - TimeUnit.DAYS.toMillis(day);

            for (int i = 0; i < 50; i++) {
                String service = services.get(random.nextInt(services.size()));
                String level = levels.get(random.nextInt(levels.size()));
                String userId = users.get(random.nextInt(users.size()));

                long timestamp = dayTimestamp - random.nextInt(86400000); // Random time within the day

                String json = String.format(
                    "{\"creationTimestamp\": %d, \"level\": \"%s\", \"service\": \"%s\", " +
                    "\"userId\": \"%s\", \"message\": \"Sample log message %d\", " +
                    "\"traceId\": \"trace-%d\"}",
                    timestamp, level, service, userId, i, random.nextInt(1000)
                );

                documents.add(json);
            }
        }

        logger.info("Ingesting {} documents across 20 days...", documents.size());

        // Ingest in batches
        int batchSize = 100;
        for (int i = 0; i < documents.size(); i += batchSize) {
            int end = Math.min(i + batchSize, documents.size());
            system.ingestBatch(documents.subList(i, end));
        }

        system.flush();
        logger.info("Ingested {} documents", documents.size());
    }

    /**
     * Run various query examples
     */
    private static void runQueryExamples(LogIngestionSystem system) {
        long now = System.currentTimeMillis();

        // Example 1: Time range query (last 7 days)
        logger.info("Example 1: Time range query (last 7 days)");
        Query.TimeRangeQuery timeQuery = system.timeRangeQuery(
            now - TimeUnit.DAYS.toMillis(7),
            now
        );
        QueryResult result1 = system.query(timeQuery);
        logger.info("  Result: {} documents in {}ms\n",
                   result1.getDocumentCount(), result1.getExecutionTimeMs());

        // Example 2: Query by log level
        logger.info("Example 2: Query ERROR logs");
        Query.KeyValueQuery errorQuery = system.keyValueQuery("level", "ERROR");
        QueryResult result2 = system.query(errorQuery);
        logger.info("  Result: {} ERROR logs in {}ms\n",
                   result2.getDocumentCount(), result2.getExecutionTimeMs());

        // Example 3: Query by service
        logger.info("Example 3: Query logs from payment-service");
        Query.KeyValueQuery serviceQuery = system.keyValueQuery("service", "payment-service");
        QueryResult result3 = system.query(serviceQuery);
        logger.info("  Result: {} payment-service logs in {}ms\n",
                   result3.getDocumentCount(), result3.getExecutionTimeMs());

        // Example 4: Composite query (time range + filter)
        logger.info("Example 4: ERROR logs from last 15 days");
        Query.CompositeQuery compositeQuery = system.compositeQuery(
            system.timeRangeQuery(now - TimeUnit.DAYS.toMillis(15), now),
            system.keyValueQuery("level", "ERROR")
        );
        QueryResult result4 = system.query(compositeQuery);
        logger.info("  Result: {} ERROR logs in {}ms\n",
                   result4.getDocumentCount(), result4.getExecutionTimeMs());

        // Example 5: Boolean AND query
        logger.info("Example 5: ERROR logs from auth-service");
        Query.BooleanQuery andQuery = Query.BooleanQuery.and(
            system.keyValueQuery("level", "ERROR"),
            system.keyValueQuery("service", "auth-service")
        );
        QueryResult result5 = system.query(andQuery);
        logger.info("  Result: {} documents in {}ms\n",
                   result5.getDocumentCount(), result5.getExecutionTimeMs());

        // Example 6: Boolean OR query
        logger.info("Example 6: ERROR or WARN logs");
        Query.BooleanQuery orQuery = Query.BooleanQuery.or(
            system.keyValueQuery("level", "ERROR"),
            system.keyValueQuery("level", "WARN")
        );
        QueryResult result6 = system.query(orQuery);
        logger.info("  Result: {} documents in {}ms\n",
                   result6.getDocumentCount(), result6.getExecutionTimeMs());

        // Display some sample results
        if (!result1.getDocuments().isEmpty()) {
            logger.info("\nSample documents from time range query:");
            for (int i = 0; i < Math.min(3, result1.getDocuments().size()); i++) {
                Document doc = result1.getDocuments().get(i);
                logger.info("  - {}", doc.getJsonContent());
            }
        }
    }

    /**
     * Show system statistics
     */
    @SuppressWarnings("unchecked")
    private static void showStatistics(LogIngestionSystem system) {
        Map<String, Object> stats = system.getStatistics();

        logger.info("Hot Tier: {}", stats.get("hotTier"));
        logger.info("Cold Tier: {}", stats.get("coldTier"));
        logger.info("Ingestion: {}", stats.get("ingestion"));
        logger.info("Lifecycle: {}", stats.get("lifecycle"));
    }

    /**
     * Test out-of-order document ingestion
     */
    private static void testOutOfOrderIngestion(LogIngestionSystem system) throws Exception {
        long now = System.currentTimeMillis();

        // Create a very old document (should go to cold tier)
        long oldTimestamp = now - TimeUnit.DAYS.toMillis(30);
        String oldDoc = String.format(
            "{\"creationTimestamp\": %d, \"level\": \"INFO\", \"service\": \"old-service\", " +
            "\"userId\": \"user999\", \"message\": \"This is an old out-of-order document\"}",
            oldTimestamp
        );

        logger.info("Ingesting document from 30 days ago...");
        system.ingest(oldDoc);
        system.flush();

        // Query to verify it went to cold tier
        Query.TimeRangeQuery oldQuery = system.timeRangeQuery(oldTimestamp - 1000, oldTimestamp + 1000);
        QueryResult result = system.query(oldQuery);

        logger.info("Verification: Found {} documents in the old time range", result.getDocumentCount());
        if (!result.getDocuments().isEmpty()) {
            logger.info("Document content: {}", result.getDocuments().get(0).getJsonContent());
        }
    }

    /**
     * Test lifecycle management (data migration)
     */
    private static void testLifecycleManagement(LogIngestionSystem system) throws Exception {
        logger.info("Forcing migration of expired documents...");

        Map<String, Object> beforeStats = system.getStatistics();
        logger.info("Before migration - Ingestion stats: {}", beforeStats.get("ingestion"));

        system.forceMigration();

        Map<String, Object> afterStats = system.getStatistics();
        logger.info("After migration - Lifecycle stats: {}", afterStats.get("lifecycle"));
    }
}
