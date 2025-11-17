package com.logingestion.index;

import com.logingestion.model.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Time-based index using ConcurrentSkipListMap (similar to B+Tree)
 * Provides efficient range queries on timestamps
 *
 * Thread-safety: Lock-free implementation using ConcurrentSkipListMap + CopyOnWriteArrayList
 * - ConcurrentSkipListMap: Thread-safe for all map operations
 * - CopyOnWriteArrayList: Thread-safe for list operations (optimal for 1-3 docs per timestamp)
 * - No locks needed: All operations are atomic and thread-safe
 *
 * Performance characteristics:
 * - Insert: O(log n) + O(k) where k = docs at same timestamp (typically 1-3)
 * - Range query: O(log n + m) where m = results, never blocked by writes
 * - Memory: Slightly higher during writes (array copy), but minimal for small lists
 */
public class TimeBasedIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TimeBasedIndex.class);

    // ConcurrentSkipListMap provides O(log n) operations similar to B+Tree
    // Key: creationTimestamp, Value: Thread-safe list of documents (typically 1-3 docs)
    private final ConcurrentSkipListMap<Long, CopyOnWriteArrayList<Document>> timeIndex;

    public TimeBasedIndex() {
        this.timeIndex = new ConcurrentSkipListMap<>();
    }

    /**
     * Add a document to the time-based index
     * Thread-safe: ConcurrentSkipListMap.computeIfAbsent is atomic,
     * CopyOnWriteArrayList.add is thread-safe
     */
    public void addDocument(Document doc) {
        long timestamp = doc.getCreationTimestamp();
        timeIndex.computeIfAbsent(timestamp, k -> new CopyOnWriteArrayList<>()).add(doc);
        logger.debug("Added document {} to time index at timestamp {}", doc.getId(), timestamp);
    }

    /**
     * Remove a document from the index
     * Thread-safe: CopyOnWriteArrayList.remove is thread-safe
     */
    public void removeDocument(Document doc) {
        long timestamp = doc.getCreationTimestamp();
        CopyOnWriteArrayList<Document> docs = timeIndex.get(timestamp);
        if (docs != null) {
            docs.remove(doc);
            if (docs.isEmpty()) {
                timeIndex.remove(timestamp);
            }
        }
        logger.debug("Removed document {} from time index", doc.getId());
    }

    /**
     * Query documents within a time range [start, end]
     * Thread-safe: Lock-free read, never blocked by writes
     */
    public List<Document> queryRange(long startTimestamp, long endTimestamp) {
        List<Document> results = new ArrayList<>();

        // Get all entries in the range - returns thread-safe view
        NavigableMap<Long, CopyOnWriteArrayList<Document>> rangeMap =
            timeIndex.subMap(startTimestamp, true, endTimestamp, true);

        // Iterate safely - CopyOnWriteArrayList provides snapshot iteration
        for (CopyOnWriteArrayList<Document> docs : rangeMap.values()) {
            results.addAll(docs);
        }

        logger.debug("Range query [{}, {}] returned {} documents",
                    startTimestamp, endTimestamp, results.size());
        return results;
    }

    /**
     * Get documents older than a specific timestamp
     * Thread-safe: Lock-free read
     */
    public List<Document> getDocumentsOlderThan(long timestamp) {
        List<Document> results = new ArrayList<>();
        NavigableMap<Long, CopyOnWriteArrayList<Document>> headMap =
            timeIndex.headMap(timestamp, false);

        for (CopyOnWriteArrayList<Document> docs : headMap.values()) {
            results.addAll(docs);
        }

        logger.debug("Query for documents older than {} returned {} documents",
                    timestamp, results.size());
        return results;
    }

    /**
     * Remove all documents older than a specific timestamp
     * Thread-safe: Atomic removes
     */
    public List<Document> removeDocumentsOlderThan(long timestamp) {
        List<Document> removed = new ArrayList<>();
        NavigableMap<Long, CopyOnWriteArrayList<Document>> headMap =
            timeIndex.headMap(timestamp, false);

        // Snapshot entries to avoid concurrent modification during iteration
        for (Map.Entry<Long, CopyOnWriteArrayList<Document>> entry :
                new ArrayList<>(headMap.entrySet())) {
            removed.addAll(entry.getValue());
            timeIndex.remove(entry.getKey());
        }

        logger.info("Removed {} documents older than timestamp {}", removed.size(), timestamp);
        return removed;
    }

    /**
     * Get the oldest timestamp in the index
     * Thread-safe: firstKey() is atomic
     */
    public Long getOldestTimestamp() {
        return timeIndex.isEmpty() ? null : timeIndex.firstKey();
    }

    /**
     * Get the newest timestamp in the index
     * Thread-safe: lastKey() is atomic
     */
    public Long getNewestTimestamp() {
        return timeIndex.isEmpty() ? null : timeIndex.lastKey();
    }

    /**
     * Get total number of documents in the index
     * Thread-safe: Snapshot count (may be slightly stale but consistent)
     */
    public int size() {
        return timeIndex.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Clear all documents from the index
     * Thread-safe: clear() is atomic
     */
    public void clear() {
        timeIndex.clear();
        logger.info("Cleared time-based index");
    }

    /**
     * Get all documents (for snapshot/persistence)
     * Thread-safe: Snapshot of all documents at time of call
     */
    public List<Document> getAllDocuments() {
        List<Document> all = new ArrayList<>();
        for (CopyOnWriteArrayList<Document> docs : timeIndex.values()) {
            all.addAll(docs);
        }
        return all;
    }
}
