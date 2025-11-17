package com.logingestion;

import com.logingestion.ingestion.DocumentIngester;
import com.logingestion.lifecycle.DataLifecycleManager;
import com.logingestion.model.IndexConfig;
import com.logingestion.model.Query;
import com.logingestion.query.QueryExecutor;
import com.logingestion.query.QueryResult;
import com.logingestion.storage.ColdTierStorage;
import com.logingestion.storage.HotTierStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main entry point for the Log Ingestion System
 * Provides a unified interface for ingestion, querying, and management
 */
public class LogIngestionSystem {
    private static final Logger logger = LoggerFactory.getLogger(LogIngestionSystem.class);

    private final IndexConfig config;
    private final HotTierStorage hotTier;
    private final ColdTierStorage coldTier;
    private final DocumentIngester ingester;
    private final QueryExecutor queryExecutor;
    private final DataLifecycleManager lifecycleManager;

    private volatile boolean running;

    /**
     * Create a new log ingestion system with default config
     */
    public LogIngestionSystem() throws IOException {
        this(IndexConfig.getDefault());
    }

    /**
     * Create a new log ingestion system with custom config
     */
    public LogIngestionSystem(IndexConfig config) throws IOException {
        this.config = config;

        logger.info("Initializing Log Ingestion System");
        logger.info("Configuration: retention={} days, indexed fields={}",
                   config.getHotTierRetentionDays(), config.getIndexedFields());

        // Initialize storage tiers
        this.hotTier = new HotTierStorage(config);
        this.coldTier = new ColdTierStorage(config);

        // Initialize ingester
        this.ingester = new DocumentIngester(hotTier, coldTier, config);

        // Initialize query executor
        this.queryExecutor = new QueryExecutor(hotTier, coldTier, config);

        // Initialize lifecycle manager
        this.lifecycleManager = new DataLifecycleManager(hotTier, coldTier, config);

        this.running = false;

        logger.info("Log Ingestion System initialized successfully");
    }

    /**
     * Start the system
     */
    public void start() {
        if (running) {
            logger.warn("System already running");
            return;
        }

        logger.info("Starting Log Ingestion System");

        ingester.start();
        lifecycleManager.start();

        running = true;

        logger.info("Log Ingestion System started");
    }

    /**
     * Ingest a document (synchronous)
     */
    public void ingest(String jsonContent) throws IOException {
        if (!running) {
            throw new IllegalStateException("System not started");
        }
        ingester.ingest(jsonContent);
    }

    /**
     * Ingest a document (asynchronous)
     */
    public void ingestAsync(String jsonContent) throws InterruptedException {
        if (!running) {
            throw new IllegalStateException("System not started");
        }
        ingester.ingestAsync(jsonContent);
    }

    /**
     * Ingest a batch of documents
     */
    public void ingestBatch(List<String> jsonContents) throws IOException {
        if (!running) {
            throw new IllegalStateException("System not started");
        }
        ingester.ingestBatch(jsonContents);
    }

    /**
     * Execute a query
     */
    public QueryResult query(Query query) {
        if (!running) {
            throw new IllegalStateException("System not started");
        }
        return queryExecutor.execute(query);
    }

    /**
     * Create a time range query
     */
    public Query.TimeRangeQuery timeRangeQuery(long startTimestamp, long endTimestamp) {
        return new Query.TimeRangeQuery(startTimestamp, endTimestamp);
    }

    /**
     * Create a key-value query
     */
    public Query.KeyValueQuery keyValueQuery(String key, String value) {
        return new Query.KeyValueQuery(key, value);
    }

    /**
     * Create a composite query (time range + filters)
     */
    public Query.CompositeQuery compositeQuery(Query.TimeRangeQuery timeRange, Query filter) {
        return new Query.CompositeQuery(timeRange, filter);
    }

    /**
     * Get system statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Hot tier stats
        stats.put("hotTier", hotTier.getStatistics());

        // Cold tier stats
        stats.put("coldTier", coldTier.getStatistics());

        // Ingestion stats
        stats.put("ingestion", ingester.getStatistics());

        // Lifecycle stats
        stats.put("lifecycle", lifecycleManager.getStatistics());

        // System info
        Map<String, Object> systemInfo = new HashMap<>();
        systemInfo.put("running", running);
        systemInfo.put("retentionDays", config.getHotTierRetentionDays());
        systemInfo.put("indexedFields", config.getIndexedFields());
        stats.put("system", systemInfo);

        return stats;
    }

    /**
     * Flush pending ingestions
     */
    public void flush() throws InterruptedException {
        ingester.flush();
    }

    /**
     * Force migration of expired documents
     */
    public void forceMigration() throws IOException {
        lifecycleManager.migrateExpiredDocuments();
    }

    /**
     * Shutdown the system gracefully
     */
    public void shutdown() {
        logger.info("Shutting down Log Ingestion System");

        running = false;

        try {
            // Stop ingester
            ingester.stop();

            // Stop lifecycle manager
            lifecycleManager.stop();

            // Stop query executor
            queryExecutor.shutdown();

            // Shutdown hot tier
            hotTier.shutdown();

            logger.info("Log Ingestion System shutdown complete");

        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }

    /**
     * Load configuration from file
     */
    public static LogIngestionSystem fromConfigFile(String configPath) throws IOException {
        IndexConfig config = IndexConfig.loadFromFile(configPath);
        return new LogIngestionSystem(config);
    }

    /**
     * Main method for demonstration
     */
    public static void main(String[] args) {
        try {
            // Create and start the system
            LogIngestionSystem system = new LogIngestionSystem();
            system.start();

            logger.info("Log Ingestion System is ready!");
            logger.info("Configuration: {}", system.config.getIndexedFields());

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook triggered");
                system.shutdown();
            }));

            // Keep the system running
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("Failed to start Log Ingestion System", e);
            System.exit(1);
        }
    }

    // Getters for testing
    public HotTierStorage getHotTier() {
        return hotTier;
    }

    public ColdTierStorage getColdTier() {
        return coldTier;
    }

    public DocumentIngester getIngester() {
        return ingester;
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public DataLifecycleManager getLifecycleManager() {
        return lifecycleManager;
    }

    public IndexConfig getConfig() {
        return config;
    }

    public boolean isRunning() {
        return running;
    }
}
