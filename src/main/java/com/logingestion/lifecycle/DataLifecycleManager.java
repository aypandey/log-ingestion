package com.logingestion.lifecycle;

import com.logingestion.model.Document;
import com.logingestion.model.IndexConfig;
import com.logingestion.storage.ColdTierStorage;
import com.logingestion.storage.HotTierStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of data between hot and cold tiers
 * Automatically moves aged data from hot to cold tier
 */
public class DataLifecycleManager {
    private static final Logger logger = LoggerFactory.getLogger(DataLifecycleManager.class);

    private final HotTierStorage hotTier;
    private final ColdTierStorage coldTier;
    private final IndexConfig config;
    private final ScheduledExecutorService scheduler;

    private volatile boolean running;
    private long totalMigratedDocuments = 0;

    public DataLifecycleManager(HotTierStorage hotTier, ColdTierStorage coldTier,
                                IndexConfig config) {
        this.hotTier = hotTier;
        this.coldTier = coldTier;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.running = false;

        logger.info("DataLifecycleManager initialized");
    }

    /**
     * Start the lifecycle management
     */
    public void start() {
        if (running) {
            logger.warn("DataLifecycleManager already running");
            return;
        }

        running = true;

        // Schedule periodic compaction/migration
        long intervalHours = config.getCompactionIntervalHours();
        scheduler.scheduleAtFixedRate(
                this::performLifecycleManagement,
                intervalHours,
                intervalHours,
                TimeUnit.HOURS
        );

        logger.info("DataLifecycleManager started, compaction interval: {} hours", intervalHours);
    }

    /**
     * Perform lifecycle management tasks
     */
    private void performLifecycleManagement() {
        try {
            logger.info("Starting lifecycle management cycle");

            // Move expired documents from hot to cold tier
            migrateExpiredDocuments();

            // Optionally: compact cold tier partitions
            // compactColdTier();

            logger.info("Lifecycle management cycle completed");

        } catch (Exception e) {
            logger.error("Lifecycle management cycle failed", e);
        }
    }

    /**
     * Migrate expired documents from hot to cold tier
     */
    public void migrateExpiredDocuments() throws IOException {
        logger.info("Checking for expired documents in hot tier");

        // Get documents older than retention period
        List<Document> expiredDocs = hotTier.getExpiredDocuments();

        if (expiredDocs.isEmpty()) {
            logger.info("No expired documents to migrate");
            return;
        }

        logger.info("Found {} expired documents to migrate", expiredDocs.size());

        // Store in cold tier
        coldTier.storeDocuments(expiredDocs);

        // Remove from hot tier
        List<Document> removed = hotTier.evictExpiredDocuments();

        totalMigratedDocuments += removed.size();

        logger.info("Migrated {} documents to cold tier (total: {})",
                   removed.size(), totalMigratedDocuments);
    }

    /**
     * Force migration of all documents older than specified timestamp
     */
    public void forceMigration(long olderThanTimestamp) throws IOException {
        logger.info("Force migrating documents older than {}", olderThanTimestamp);

        // This would require additional API in HotTierStorage
        // For now, just trigger normal migration
        migrateExpiredDocuments();
    }

    /**
     * Get statistics
     */
    public LifecycleStats getStatistics() {
        return new LifecycleStats(
                running,
                totalMigratedDocuments,
                config.getHotTierRetentionDays(),
                config.getCompactionIntervalHours()
        );
    }

    /**
     * Stop the lifecycle manager
     */
    public void stop() {
        logger.info("Stopping DataLifecycleManager");

        running = false;
        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }

            // Perform one final migration
            migrateExpiredDocuments();

            logger.info("DataLifecycleManager stopped");

        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            logger.error("DataLifecycleManager shutdown interrupted", e);
        } catch (IOException e) {
            logger.error("Final migration failed", e);
        }
    }

    /**
     * Lifecycle statistics
     */
    public static class LifecycleStats {
        private final boolean running;
        private final long totalMigratedDocuments;
        private final int retentionDays;
        private final int compactionIntervalHours;

        public LifecycleStats(boolean running, long totalMigratedDocuments,
                            int retentionDays, int compactionIntervalHours) {
            this.running = running;
            this.totalMigratedDocuments = totalMigratedDocuments;
            this.retentionDays = retentionDays;
            this.compactionIntervalHours = compactionIntervalHours;
        }

        public boolean isRunning() {
            return running;
        }

        public long getTotalMigratedDocuments() {
            return totalMigratedDocuments;
        }

        public int getRetentionDays() {
            return retentionDays;
        }

        public int getCompactionIntervalHours() {
            return compactionIntervalHours;
        }

        @Override
        public String toString() {
            return String.format("LifecycleStats{running=%s, migrated=%d, retention=%d days, interval=%d hours}",
                    running, totalMigratedDocuments, retentionDays, compactionIntervalHours);
        }
    }
}
