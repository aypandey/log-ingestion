package com.logingestion.ingestion;

import com.logingestion.model.Document;
import com.logingestion.model.IndexConfig;
import com.logingestion.storage.ColdTierStorage;
import com.logingestion.storage.HotTierStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Document ingester - handles incoming documents with out-of-order arrival
 * Routes documents to appropriate tier based on timestamp
 */
public class DocumentIngester {
    private static final Logger logger = LoggerFactory.getLogger(DocumentIngester.class);

    private final HotTierStorage hotTier;
    private final ColdTierStorage coldTier;
    private final IndexConfig config;
    private final ExecutorService ingestionExecutor;
    private final BlockingQueue<Document> ingestionQueue;

    private final AtomicLong totalIngested;
    private final AtomicLong hotTierIngested;
    private final AtomicLong coldTierIngested;
    private final AtomicLong outOfOrderCount;

    private volatile boolean running;

    public DocumentIngester(HotTierStorage hotTier, ColdTierStorage coldTier,
                           IndexConfig config) {
        this.hotTier = hotTier;
        this.coldTier = coldTier;
        this.config = config;
        this.ingestionExecutor = Executors.newFixedThreadPool(4);
        this.ingestionQueue = new LinkedBlockingQueue<>(10000); // Buffer for async ingestion

        this.totalIngested = new AtomicLong(0);
        this.hotTierIngested = new AtomicLong(0);
        this.coldTierIngested = new AtomicLong(0);
        this.outOfOrderCount = new AtomicLong(0);

        this.running = false;

        logger.info("DocumentIngester initialized");
    }

    /**
     * Start the ingester
     */
    public void start() {
        if (running) {
            logger.warn("DocumentIngester already running");
            return;
        }

        running = true;

        // Start background workers to process ingestion queue
        for (int i = 0; i < 4; i++) {
            ingestionExecutor.submit(this::processIngestionQueue);
        }

        logger.info("DocumentIngester started with 4 workers");
    }

    /**
     * Ingest a single document (synchronous)
     */
    public void ingest(String jsonContent) throws IOException {
        Document doc = new Document(jsonContent);
        ingestDocument(doc);
    }

    /**
     * Ingest a single document (asynchronous)
     */
    public void ingestAsync(String jsonContent) throws InterruptedException {
        Document doc = new Document(jsonContent);
        ingestionQueue.put(doc);
    }

    /**
     * Ingest a batch of documents
     */
    public void ingestBatch(List<String> jsonContents) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (String json : jsonContents) {
            documents.add(new Document(json));
        }
        ingestDocuments(documents);
    }

    /**
     * Process a single document and route to appropriate tier
     */
    private void ingestDocument(Document doc) throws IOException {
        long now = System.currentTimeMillis();
        long docTimestamp = doc.getCreationTimestamp();
        long hotTierCutoff = now - config.getHotTierRetentionMillis();

        // Check if document is out of order
        if (docTimestamp > now) {
            logger.warn("Document has future timestamp: {} vs now: {}", docTimestamp, now);
            outOfOrderCount.incrementAndGet();
        } else if (docTimestamp < now - (365L * 24 * 60 * 60 * 1000)) {
            logger.warn("Document is very old (>1 year): {}", docTimestamp);
            outOfOrderCount.incrementAndGet();
        }

        // Route to appropriate tier based on timestamp
        if (docTimestamp >= hotTierCutoff) {
            // Document belongs to hot tier
            hotTier.addDocument(doc);
            hotTierIngested.incrementAndGet();
            logger.debug("Ingested document {} to hot tier", doc.getId());
        } else {
            // Document belongs to cold tier (out-of-order old document)
            List<Document> docs = List.of(doc);
            coldTier.storeDocuments(docs);
            coldTierIngested.incrementAndGet();
            outOfOrderCount.incrementAndGet();
            logger.debug("Ingested out-of-order document {} to cold tier", doc.getId());
        }

        totalIngested.incrementAndGet();
    }

    /**
     * Process multiple documents
     */
    private void ingestDocuments(List<Document> documents) throws IOException {
        long now = System.currentTimeMillis();
        long hotTierCutoff = now - config.getHotTierRetentionMillis();

        List<Document> hotDocs = new ArrayList<>();
        List<Document> coldDocs = new ArrayList<>();

        // Partition documents by tier
        for (Document doc : documents) {
            if (doc.getCreationTimestamp() >= hotTierCutoff) {
                hotDocs.add(doc);
            } else {
                coldDocs.add(doc);
                outOfOrderCount.incrementAndGet();
            }
        }

        // Ingest to respective tiers
        if (!hotDocs.isEmpty()) {
            hotTier.addDocuments(hotDocs);
            hotTierIngested.addAndGet(hotDocs.size());
        }

        if (!coldDocs.isEmpty()) {
            coldTier.storeDocuments(coldDocs);
            coldTierIngested.addAndGet(coldDocs.size());
        }

        totalIngested.addAndGet(documents.size());

        logger.info("Ingested batch: {} to hot tier, {} to cold tier",
                   hotDocs.size(), coldDocs.size());
    }

    /**
     * Background worker to process ingestion queue
     */
    private void processIngestionQueue() {
        logger.info("Ingestion worker started");

        while (running || !ingestionQueue.isEmpty()) {
            try {
                Document doc = ingestionQueue.poll(1, TimeUnit.SECONDS);
                if (doc != null) {
                    ingestDocument(doc);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Failed to ingest document", e);
            }
        }

        logger.info("Ingestion worker stopped");
    }

    /**
     * Get ingestion statistics
     */
    public IngestionStats getStatistics() {
        return new IngestionStats(
                totalIngested.get(),
                hotTierIngested.get(),
                coldTierIngested.get(),
                outOfOrderCount.get(),
                ingestionQueue.size()
        );
    }

    /**
     * Wait for queue to be empty
     */
    public void flush() throws InterruptedException {
        logger.info("Flushing ingestion queue ({} items)", ingestionQueue.size());

        while (!ingestionQueue.isEmpty()) {
            Thread.sleep(100);
        }

        logger.info("Ingestion queue flushed");
    }

    /**
     * Stop the ingester
     */
    public void stop() throws InterruptedException {
        logger.info("Stopping DocumentIngester");

        running = false;

        // Wait for queue to be processed
        flush();

        // Shutdown executor
        ingestionExecutor.shutdown();
        if (!ingestionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            ingestionExecutor.shutdownNow();
        }

        logger.info("DocumentIngester stopped. Total ingested: {}", totalIngested.get());
    }

    /**
     * Ingestion statistics
     */
    public static class IngestionStats {
        private final long totalIngested;
        private final long hotTierIngested;
        private final long coldTierIngested;
        private final long outOfOrderCount;
        private final int queueSize;

        public IngestionStats(long totalIngested, long hotTierIngested,
                            long coldTierIngested, long outOfOrderCount, int queueSize) {
            this.totalIngested = totalIngested;
            this.hotTierIngested = hotTierIngested;
            this.coldTierIngested = coldTierIngested;
            this.outOfOrderCount = outOfOrderCount;
            this.queueSize = queueSize;
        }

        public long getTotalIngested() {
            return totalIngested;
        }

        public long getHotTierIngested() {
            return hotTierIngested;
        }

        public long getColdTierIngested() {
            return coldTierIngested;
        }

        public long getOutOfOrderCount() {
            return outOfOrderCount;
        }

        public int getQueueSize() {
            return queueSize;
        }

        @Override
        public String toString() {
            return String.format("IngestionStats{total=%d, hot=%d, cold=%d, outOfOrder=%d, queue=%d}",
                    totalIngested, hotTierIngested, coldTierIngested, outOfOrderCount, queueSize);
        }
    }
}
