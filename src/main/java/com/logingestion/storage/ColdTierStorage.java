package com.logingestion.storage;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.logingestion.model.Document;
import com.logingestion.model.IndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Cold tier storage - stores older data on filesystem
 * Data is partitioned by date for efficient range queries
 * Maintains lightweight metadata index for faster lookups
 */
public class ColdTierStorage {
    private static final Logger logger = LoggerFactory.getLogger(ColdTierStorage.class);
    private static final String DATA_FILE_SUFFIX = ".jsonl"; // JSON Lines format
    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final Gson gson = new Gson();

    private final Path coldStoragePath;
    private final IndexConfig config;
    private final ReadWriteLock lock;

    // Lightweight metadata index: datePartition -> Map<field -> Map<value -> List<docId>>>
    private final Map<String, Map<String, Map<String, List<String>>>> metadataIndex;

    // Track which partitions exist
    private final Set<String> partitions;

    public ColdTierStorage(IndexConfig config) throws IOException {
        this.config = config;
        this.coldStoragePath = Paths.get(config.getColdStoragePath());
        this.lock = new ReentrantReadWriteLock();
        this.metadataIndex = new ConcurrentHashMap<>();
        this.partitions = ConcurrentHashMap.newKeySet();

        // Create cold storage directory
        Files.createDirectories(coldStoragePath);

        // Load existing partitions and metadata
        loadExistingPartitions();

        logger.info("ColdTierStorage initialized at {} with {} partitions",
                   coldStoragePath, partitions.size());
    }

    /**
     * Load existing partitions and rebuild metadata index
     */
    private void loadExistingPartitions() throws IOException {
        try (Stream<Path> paths = Files.list(coldStoragePath)) {
            List<Path> dataFiles = paths
                    .filter(p -> p.toString().endsWith(DATA_FILE_SUFFIX))
                    .collect(Collectors.toList());

            for (Path dataFile : dataFiles) {
                String partition = extractPartitionFromFileName(dataFile.getFileName().toString());
                partitions.add(partition);
                loadMetadataIndex(partition);
            }
        }

        logger.info("Loaded {} existing partitions", partitions.size());
    }

    /**
     * Store documents in cold tier
     * Documents are partitioned by date (YYYY-MM-DD)
     */
    public void storeDocuments(List<Document> documents) throws IOException {
        lock.writeLock().lock();
        try {
            // Group documents by date partition
            Map<String, List<Document>> partitionedDocs = documents.stream()
                    .collect(Collectors.groupingBy(this::getPartitionKey));

            for (Map.Entry<String, List<Document>> entry : partitionedDocs.entrySet()) {
                String partition = entry.getKey();
                List<Document> docs = entry.getValue();

                appendToPartition(partition, docs);
                updateMetadataIndex(partition, docs);
                partitions.add(partition);
            }

            logger.info("Stored {} documents in cold tier across {} partitions",
                       documents.size(), partitionedDocs.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Query documents by time range
     */
    public List<Document> queryByTimeRange(long startTimestamp, long endTimestamp) throws IOException {
        lock.readLock().lock();
        try {
            // Determine which partitions to scan
            Set<String> relevantPartitions = getRelevantPartitions(startTimestamp, endTimestamp);

            List<Document> results = new ArrayList<>();
            for (String partition : relevantPartitions) {
                results.addAll(scanPartition(partition, startTimestamp, endTimestamp));
            }

            logger.debug("Cold tier range query returned {} documents from {} partitions",
                        results.size(), relevantPartitions.size());
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Query documents by field value within a time range
     */
    public List<Document> queryByFieldValue(String field, String value,
                                           long startTimestamp, long endTimestamp) throws IOException {
        lock.readLock().lock();
        try {
            Set<String> relevantPartitions = getRelevantPartitions(startTimestamp, endTimestamp);
            List<Document> results = new ArrayList<>();

            for (String partition : relevantPartitions) {
                // Use metadata index to filter candidates
                Set<String> candidateDocIds = getCandidateDocIds(partition, field, value);

                if (!candidateDocIds.isEmpty()) {
                    // Scan partition and filter by doc IDs
                    List<Document> partitionDocs = scanPartitionForDocIds(
                            partition, candidateDocIds, startTimestamp, endTimestamp);
                    results.addAll(partitionDocs);
                }
            }

            logger.debug("Cold tier field query returned {} documents", results.size());
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get partition key (date) from document timestamp
     */
    private String getPartitionKey(Document doc) {
        Date date = new Date(doc.getCreationTimestamp());
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }

    /**
     * Get partition key from timestamp
     */
    private String getPartitionKey(long timestamp) {
        Date date = new Date(timestamp);
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }

    /**
     * Append documents to a partition file
     */
    private void appendToPartition(String partition, List<Document> documents) throws IOException {
        Path dataFile = coldStoragePath.resolve(partition + DATA_FILE_SUFFIX);

        try (BufferedWriter writer = Files.newBufferedWriter(dataFile,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            for (Document doc : documents) {
                // Write document as JSON line
                String jsonLine = gson.toJson(Map.of(
                        "id", doc.getId(),
                        "timestamp", doc.getCreationTimestamp(),
                        "content", doc.getJsonContent()
                ));
                writer.write(jsonLine);
                writer.newLine();
            }
        }
    }

    /**
     * Update metadata index for a partition
     */
    private void updateMetadataIndex(String partition, List<Document> documents) {
        Map<String, Map<String, List<String>>> partitionIndex =
                metadataIndex.computeIfAbsent(partition, k -> new ConcurrentHashMap<>());

        for (Document doc : documents) {
            for (String field : config.getIndexedFields()) {
                String value = doc.getFieldValue(field);
                if (value != null && !value.isEmpty()) {
                    partitionIndex
                            .computeIfAbsent(field, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(value, k -> new ArrayList<>())
                            .add(doc.getId());
                }
            }
        }

        // Persist metadata index
        try {
            persistMetadataIndex(partition, partitionIndex);
        } catch (IOException e) {
            logger.error("Failed to persist metadata index for partition {}", partition, e);
        }
    }

    /**
     * Persist metadata index to disk
     */
    private void persistMetadataIndex(String partition,
                                     Map<String, Map<String, List<String>>> partitionIndex)
            throws IOException {
        Path indexFile = coldStoragePath.resolve(partition + INDEX_FILE_SUFFIX);
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(indexFile.toFile())))) {
            oos.writeObject(partitionIndex);
        }
    }

    /**
     * Load metadata index from disk
     */
    @SuppressWarnings("unchecked")
    private void loadMetadataIndex(String partition) throws IOException {
        Path indexFile = coldStoragePath.resolve(partition + INDEX_FILE_SUFFIX);

        if (!Files.exists(indexFile)) {
            // Rebuild index by scanning data file
            rebuildMetadataIndex(partition);
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(indexFile.toFile())))) {
            Map<String, Map<String, List<String>>> partitionIndex =
                    (Map<String, Map<String, List<String>>>) ois.readObject();
            metadataIndex.put(partition, partitionIndex);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to load metadata index for partition {}", partition, e);
            rebuildMetadataIndex(partition);
        }
    }

    /**
     * Rebuild metadata index by scanning data file
     */
    private void rebuildMetadataIndex(String partition) throws IOException {
        logger.info("Rebuilding metadata index for partition {}", partition);

        Path dataFile = coldStoragePath.resolve(partition + DATA_FILE_SUFFIX);
        if (!Files.exists(dataFile)) {
            return;
        }

        Map<String, Map<String, List<String>>> partitionIndex = new ConcurrentHashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(dataFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                String docId = obj.get("id").getAsString();
                String content = obj.get("content").getAsString();
                JsonObject contentJson = JsonParser.parseString(content).getAsJsonObject();

                for (String field : config.getIndexedFields()) {
                    if (contentJson.has(field) && !contentJson.get(field).isJsonNull()) {
                        String value = contentJson.get(field).getAsString();
                        partitionIndex
                                .computeIfAbsent(field, k -> new ConcurrentHashMap<>())
                                .computeIfAbsent(value, k -> new ArrayList<>())
                                .add(docId);
                    }
                }
            }
        }

        metadataIndex.put(partition, partitionIndex);
        persistMetadataIndex(partition, partitionIndex);
    }

    /**
     * Get relevant partitions for a time range
     */
    private Set<String> getRelevantPartitions(long startTimestamp, long endTimestamp) {
        Set<String> relevant = new HashSet<>();

        // Generate all date partitions in the range
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(startTimestamp);

        Calendar endCal = Calendar.getInstance();
        endCal.setTimeInMillis(endTimestamp);

        while (!cal.after(endCal)) {
            String partition = getPartitionKey(cal.getTimeInMillis());
            if (partitions.contains(partition)) {
                relevant.add(partition);
            }
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        return relevant;
    }

    /**
     * Scan a partition for documents within time range
     */
    private List<Document> scanPartition(String partition, long startTimestamp, long endTimestamp)
            throws IOException {
        Path dataFile = coldStoragePath.resolve(partition + DATA_FILE_SUFFIX);
        if (!Files.exists(dataFile)) {
            return Collections.emptyList();
        }

        List<Document> results = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(dataFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                String docId = obj.get("id").getAsString();
                long timestamp = obj.get("timestamp").getAsLong();
                String content = obj.get("content").getAsString();

                if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
                    Document doc = new Document(docId, timestamp, content);
                    results.add(doc);
                }
            }
        }

        return results;
    }

    /**
     * Get candidate document IDs from metadata index
     */
    private Set<String> getCandidateDocIds(String partition, String field, String value) {
        Map<String, Map<String, List<String>>> partitionIndex = metadataIndex.get(partition);
        if (partitionIndex == null) {
            return Collections.emptySet();
        }

        Map<String, List<String>> fieldIndex = partitionIndex.get(field);
        if (fieldIndex == null) {
            return Collections.emptySet();
        }

        List<String> docIds = fieldIndex.get(value);
        return docIds == null ? Collections.emptySet() : new HashSet<>(docIds);
    }

    /**
     * Scan partition for specific document IDs
     */
    private List<Document> scanPartitionForDocIds(String partition, Set<String> docIds,
                                                  long startTimestamp, long endTimestamp)
            throws IOException {
        Path dataFile = coldStoragePath.resolve(partition + DATA_FILE_SUFFIX);
        if (!Files.exists(dataFile)) {
            return Collections.emptyList();
        }

        List<Document> results = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(dataFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                String docId = obj.get("id").getAsString();

                if (docIds.contains(docId)) {
                    long timestamp = obj.get("timestamp").getAsLong();
                    if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
                        String content = obj.get("content").getAsString();
                        Document doc = new Document(docId, timestamp, content);
                        results.add(doc);
                    }
                }
            }
        }

        return results;
    }

    /**
     * Extract partition name from file name
     */
    private String extractPartitionFromFileName(String fileName) {
        return fileName.replace(DATA_FILE_SUFFIX, "").replace(INDEX_FILE_SUFFIX, "");
    }

    /**
     * Get statistics
     */
    public Map<String, Object> getStatistics() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            stats.put("totalPartitions", partitions.size());
            stats.put("partitions", new ArrayList<>(partitions));
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }
}
