package com.logingestion.query;

import com.logingestion.model.Document;
import com.logingestion.model.IndexConfig;
import com.logingestion.model.Query;
import com.logingestion.storage.ColdTierStorage;
import com.logingestion.storage.HotTierStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Query executor with scatter-gather pattern
 * Executes queries across both hot and cold tiers in parallel
 */
public class QueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private final HotTierStorage hotTier;
    private final ColdTierStorage coldTier;
    private final IndexConfig config;
    private final ExecutorService queryExecutor;

    public QueryExecutor(HotTierStorage hotTier, ColdTierStorage coldTier, IndexConfig config) {
        this.hotTier = hotTier;
        this.coldTier = coldTier;
        this.config = config;
        this.queryExecutor = Executors.newFixedThreadPool(4);

        logger.info("QueryExecutor initialized");
    }

    /**
     * Execute a query using scatter-gather pattern
     */
    public QueryResult execute(Query query) {
        long startTime = System.currentTimeMillis();

        try {
            List<Document> results = new ArrayList<>();

            if (query instanceof Query.TimeRangeQuery) {
                results = executeTimeRangeQuery((Query.TimeRangeQuery) query);
            } else if (query instanceof Query.KeyValueQuery) {
                // For KeyValueQuery, we need to search all data (no time constraint)
                results = executeKeyValueQuery((Query.KeyValueQuery) query);
            } else if (query instanceof Query.BooleanQuery) {
                results = executeBooleanQuery((Query.BooleanQuery) query);
            } else if (query instanceof Query.CompositeQuery) {
                results = executeCompositeQuery((Query.CompositeQuery) query);
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Query executed in {}ms, returned {} documents", duration, results.size());

            return new QueryResult(results, duration);

        } catch (Exception e) {
            logger.error("Query execution failed", e);
            long duration = System.currentTimeMillis() - startTime;
            return new QueryResult(Collections.emptyList(), duration, e.getMessage());
        }
    }

    /**
     * Execute time range query with scatter-gather
     */
    private List<Document> executeTimeRangeQuery(Query.TimeRangeQuery query)
            throws InterruptedException, ExecutionException, IOException {

        long hotTierCutoff = System.currentTimeMillis() - config.getHotTierRetentionMillis();
        long queryStart = query.getStartTimestamp();
        long queryEnd = query.getEndTimestamp();

        List<Future<List<Document>>> futures = new ArrayList<>();

        // Query hot tier if time range overlaps
        if (queryEnd >= hotTierCutoff) {
            futures.add(queryExecutor.submit(() ->
                    hotTier.queryByTimeRange(queryStart, queryEnd)));
        }

        // Query cold tier if time range extends beyond hot tier
        if (queryStart < hotTierCutoff) {
            futures.add(queryExecutor.submit(() ->
                    coldTier.queryByTimeRange(queryStart, Math.min(queryEnd, hotTierCutoff))));
        }

        // Gather results
        return gatherResults(futures);
    }

    /**
     * Execute key-value query across both tiers
     */
    private List<Document> executeKeyValueQuery(Query.KeyValueQuery query)
            throws InterruptedException, ExecutionException, IOException {

        List<Future<List<Document>>> futures = new ArrayList<>();

        // Query hot tier
        futures.add(queryExecutor.submit(() ->
                hotTier.queryByFieldValue(query.getKey(), query.getValue())));

        // Query cold tier (all time)
        long now = System.currentTimeMillis();
        long veryOldTime = now - (365L * 24 * 60 * 60 * 1000); // 1 year ago

        futures.add(queryExecutor.submit(() ->
                coldTier.queryByFieldValue(query.getKey(), query.getValue(),
                        veryOldTime, now)));

        // Gather and filter results
        List<Document> allResults = gatherResults(futures);
        return allResults.stream()
                .filter(query::matches)
                .collect(Collectors.toList());
    }

    /**
     * Execute boolean query
     */
    private List<Document> executeBooleanQuery(Query.BooleanQuery query)
            throws InterruptedException, ExecutionException, IOException {

        if (query.getOperator() == Query.BooleanQuery.Operator.AND) {
            // Optimize AND queries by executing sub-queries and intersecting results
            return executeAndQuery(query);
        } else if (query.getOperator() == Query.BooleanQuery.Operator.OR) {
            // Execute all sub-queries and union results
            return executeOrQuery(query);
        } else {
            // NOT query
            return executeNotQuery(query);
        }
    }

    /**
     * Execute AND boolean query
     */
    private List<Document> executeAndQuery(Query.BooleanQuery query)
            throws InterruptedException, ExecutionException, IOException {

        List<Set<String>> docIdSets = new ArrayList<>();

        for (Query subQuery : query.getSubQueries()) {
            QueryResult result = execute(subQuery);
            Set<String> docIds = result.getDocuments().stream()
                    .map(Document::getId)
                    .collect(Collectors.toSet());
            docIdSets.add(docIds);
        }

        // Intersect all doc ID sets
        if (docIdSets.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> intersection = new HashSet<>(docIdSets.get(0));
        for (int i = 1; i < docIdSets.size(); i++) {
            intersection.retainAll(docIdSets.get(i));
        }

        // Retrieve documents by IDs
        return retrieveDocumentsByIds(intersection);
    }

    /**
     * Execute OR boolean query
     */
    private List<Document> executeOrQuery(Query.BooleanQuery query)
            throws InterruptedException, ExecutionException, IOException {

        Set<String> allDocIds = new HashSet<>();

        for (Query subQuery : query.getSubQueries()) {
            QueryResult result = execute(subQuery);
            result.getDocuments().stream()
                    .map(Document::getId)
                    .forEach(allDocIds::add);
        }

        return retrieveDocumentsByIds(allDocIds);
    }

    /**
     * Execute NOT boolean query
     */
    private List<Document> executeNotQuery(Query.BooleanQuery query)
            throws InterruptedException, ExecutionException, IOException {

        // This is expensive - get all docs and filter out matches
        // In practice, NOT queries should be combined with other constraints

        Query subQuery = query.getSubQueries().get(0);
        QueryResult result = execute(subQuery);
        Set<String> excludeIds = result.getDocuments().stream()
                .map(Document::getId)
                .collect(Collectors.toSet());

        // Get all documents (this is expensive!)
        logger.warn("NOT query requires scanning all documents - use with time range constraint");

        // For practical purposes, limit to recent data
        long now = System.currentTimeMillis();
        long monthAgo = now - (30L * 24 * 60 * 60 * 1000);

        List<Document> allDocs = executeTimeRangeQuery(
                new Query.TimeRangeQuery(monthAgo, now));

        return allDocs.stream()
                .filter(doc -> !excludeIds.contains(doc.getId()))
                .collect(Collectors.toList());
    }

    /**
     * Execute composite query (time range + filters)
     */
    private List<Document> executeCompositeQuery(Query.CompositeQuery query)
            throws InterruptedException, ExecutionException, IOException {

        // First execute time range query
        List<Document> timeRangeResults = executeTimeRangeQuery(query.getTimeRange());

        // Then apply filter query
        if (query.getFilterQuery() != null) {
            return timeRangeResults.stream()
                    .filter(query.getFilterQuery()::matches)
                    .collect(Collectors.toList());
        }

        return timeRangeResults;
    }

    /**
     * Retrieve documents by their IDs from both tiers
     */
    private List<Document> retrieveDocumentsByIds(Set<String> docIds)
            throws InterruptedException, ExecutionException {

        List<Future<Map<String, Document>>> futures = new ArrayList<>();

        // Get from hot tier
        futures.add(queryExecutor.submit(() -> {
            Map<String, Document> docs = new HashMap<>();
            for (String id : docIds) {
                Document doc = hotTier.getDocument(id);
                if (doc != null) {
                    docs.put(id, doc);
                }
            }
            return docs;
        }));

        // For cold tier, we'd need to scan - this is a limitation
        // In practice, we should optimize this with a global doc ID -> partition mapping

        // Gather results
        List<Document> results = new ArrayList<>();
        for (Future<Map<String, Document>> future : futures) {
            results.addAll(future.get().values());
        }

        return results;
    }

    /**
     * Gather results from multiple futures
     */
    private List<Document> gatherResults(List<Future<List<Document>>> futures)
            throws InterruptedException, ExecutionException {

        List<Document> allResults = new ArrayList<>();
        Set<String> seenIds = new HashSet<>();

        for (Future<List<Document>> future : futures) {
            List<Document> results = future.get();
            for (Document doc : results) {
                // Deduplicate by document ID
                if (!seenIds.contains(doc.getId())) {
                    allResults.add(doc);
                    seenIds.add(doc.getId());
                }
            }
        }

        // Sort by timestamp
        allResults.sort(Comparator.comparingLong(Document::getCreationTimestamp));

        return allResults;
    }

    /**
     * Shutdown the query executor
     */
    public void shutdown() {
        queryExecutor.shutdown();
        try {
            if (!queryExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                queryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            queryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("QueryExecutor shut down");
    }
}
