package com.logingestion.storage;

import com.logingestion.index.InvertedIndex;
import com.logingestion.index.TimeBasedIndex;
import com.logingestion.model.Document;
import com.logingestion.model.IndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hot tier storage - keeps recent data (last N days) in memory
 * Uses WAL for durability and periodic snapshots for faster recovery
 */
public class HotTierStorage {
    private static final Logger logger = LoggerFactory.getLogger(HotTierStorage.class);

    private final IndexConfig config;
    private final TimeBasedIndex timeIndex;
    private final InvertedIndex invertedIndex;
    private final WAL wal;
    private final SnapshotManager snapshotManager;
    private final Map<String, Document> documentStore; // documentId -> Document
    private final ReadWriteLock lock;
    private final ScheduledExecutorService snapshotExecutor;

    public HotTierStorage(IndexConfig config) throws IOException {
        this.config = config;
        this.timeIndex = new TimeBasedIndex();
        this.invertedIndex = new InvertedIndex(config.getIndexedFields());
        this.wal = new WAL(config.getWalPath());
        this.snapshotManager = new SnapshotManager(config.getSnapshotPath());
        this.documentStore = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.snapshotExecutor = Executors.newSingleThreadScheduledExecutor();

        // Recover from snapshot and WAL
        recover();

        // Schedule periodic snapshots
        scheduleSnapshots();

        logger.info("HotTierStorage initialized with {} documents",
                   documentStore.size());
    }

    /**
     * Recover state from snapshot and WAL
     */
    private void recover() throws IOException {
        try {
            // Try to load latest snapshot
            SnapshotManager.SnapshotData snapshot = snapshotManager.loadLatestSnapshot();

            if (snapshot != null) {
                logger.info("Recovered from snapshot");
                // Note: We create new indexes, so we need to rebuild them from snapshot data
                for (Document doc : snapshot.getTimeIndex().getAllDocuments()) {
                    documentStore.put(doc.getId(), doc);
                    timeIndex.addDocument(doc);
                    invertedIndex.addDocument(doc);
                }
            }

            // Replay WAL to get any operations after the snapshot
            List<WAL.WALEntry> walEntries = wal.replay();
            for (WAL.WALEntry entry : walEntries) {
                if (entry.isInsert()) {
                    Document doc = entry.getDocument();
                    documentStore.put(doc.getId(), doc);
                    timeIndex.addDocument(doc);
                    invertedIndex.addDocument(doc);
                } else if (entry.isDelete()) {
                    String docId = entry.getDeletedDocId();
                    Document doc = documentStore.remove(docId);
                    if (doc != null) {
                        timeIndex.removeDocument(doc);
                        invertedIndex.removeDocument(doc);
                    }
                }
            }

            logger.info("Replayed {} WAL entries", walEntries.size());

        } catch (ClassNotFoundException e) {
            logger.error("Failed to recover from snapshot", e);
            throw new IOException("Recovery failed", e);
        }
    }

    /**
     * Schedule periodic snapshots
     */
    private void scheduleSnapshots() {
        long intervalMinutes = config.getHotTierSnapshotIntervalMinutes();
        snapshotExecutor.scheduleAtFixedRate(() -> {
            try {
                createSnapshot();
            } catch (Exception e) {
                logger.error("Snapshot creation failed", e);
            }
        }, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);

        logger.info("Scheduled snapshots every {} minutes", intervalMinutes);
    }

    /**
     * Add a document to hot tier storage
     */
    public void addDocument(Document doc) throws IOException {
        lock.writeLock().lock();
        try {
            // Write to WAL first for durability
            wal.logInsert(doc);

            // Add to in-memory structures
            documentStore.put(doc.getId(), doc);
            timeIndex.addDocument(doc);
            invertedIndex.addDocument(doc);

            logger.debug("Added document {} to hot tier", doc.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add multiple documents in batch
     */
    public void addDocuments(List<Document> documents) throws IOException {
        lock.writeLock().lock();
        try {
            for (Document doc : documents) {
                wal.logInsert(doc);
                documentStore.put(doc.getId(), doc);
                timeIndex.addDocument(doc);
                invertedIndex.addDocument(doc);
            }
            logger.info("Added {} documents to hot tier", documents.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Query documents by time range
     */
    public List<Document> queryByTimeRange(long startTimestamp, long endTimestamp) {
        lock.readLock().lock();
        try {
            return timeIndex.queryRange(startTimestamp, endTimestamp);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Query documents by field value
     */
    public List<Document> queryByFieldValue(String field, String value) {
        lock.readLock().lock();
        try {
            Set<String> docIds = invertedIndex.query(field, value);
            List<Document> results = new ArrayList<>();
            for (String docId : docIds) {
                Document doc = documentStore.get(docId);
                if (doc != null) {
                    results.add(doc);
                }
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Query documents by multiple field values (AND operation)
     */
    public List<Document> queryByMultipleFields(Map<String, String> fieldValues) {
        lock.readLock().lock();
        try {
            Set<String> docIds = invertedIndex.queryMultiple(fieldValues);
            List<Document> results = new ArrayList<>();
            for (String docId : docIds) {
                Document doc = documentStore.get(docId);
                if (doc != null) {
                    results.add(doc);
                }
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get document by ID
     */
    public Document getDocument(String documentId) {
        lock.readLock().lock();
        try {
            return documentStore.get(documentId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get documents older than the retention period
     */
    public List<Document> getExpiredDocuments() {
        lock.readLock().lock();
        try {
            long cutoffTime = System.currentTimeMillis() - config.getHotTierRetentionMillis();
            return timeIndex.getDocumentsOlderThan(cutoffTime);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Remove documents older than the retention period
     * Returns the removed documents for migration to cold tier
     */
    public List<Document> evictExpiredDocuments() throws IOException {
        lock.writeLock().lock();
        try {
            long cutoffTime = System.currentTimeMillis() - config.getHotTierRetentionMillis();
            List<Document> expiredDocs = timeIndex.removeDocumentsOlderThan(cutoffTime);

            // Remove from other structures
            for (Document doc : expiredDocs) {
                documentStore.remove(doc.getId());
                invertedIndex.removeDocument(doc);
                wal.logDelete(doc.getId());
            }

            logger.info("Evicted {} expired documents from hot tier", expiredDocs.size());
            return expiredDocs;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create a snapshot
     */
    public void createSnapshot() throws IOException {
        lock.readLock().lock();
        try {
            logger.info("Creating snapshot...");
            snapshotManager.createSnapshot(timeIndex, invertedIndex);

            // After successful snapshot, truncate WAL
            wal.truncate();

            logger.info("Snapshot created and WAL truncated");
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get statistics
     */
    public Map<String, Object> getStatistics() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            stats.put("totalDocuments", documentStore.size());
            stats.put("oldestTimestamp", timeIndex.getOldestTimestamp());
            stats.put("newestTimestamp", timeIndex.getNewestTimestamp());
            stats.put("indexedFields", invertedIndex.getIndexedFields());
            stats.put("totalIndexTerms", invertedIndex.getTotalTerms());
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Shutdown the hot tier storage
     */
    public void shutdown() throws IOException {
        logger.info("Shutting down HotTierStorage...");

        // Stop snapshot scheduler
        snapshotExecutor.shutdown();
        try {
            if (!snapshotExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                snapshotExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Create final snapshot
        try {
            createSnapshot();
        } catch (Exception e) {
            logger.error("Failed to create final snapshot", e);
        }

        // Close WAL
        wal.close();

        logger.info("HotTierStorage shutdown complete");
    }

    public TimeBasedIndex getTimeIndex() {
        return timeIndex;
    }

    public InvertedIndex getInvertedIndex() {
        return invertedIndex;
    }
}
