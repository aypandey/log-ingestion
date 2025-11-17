package com.logingestion.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents different types of queries supported by the system
 */
public abstract class Query {

    /**
     * Time range query - finds documents within a time range
     */
    public static class TimeRangeQuery extends Query {
        private final long startTimestamp;
        private final long endTimestamp;

        public TimeRangeQuery(long startTimestamp, long endTimestamp) {
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public long getEndTimestamp() {
            return endTimestamp;
        }

        public boolean matches(Document doc) {
            long ts = doc.getCreationTimestamp();
            return ts >= startTimestamp && ts <= endTimestamp;
        }

        @Override
        public String toString() {
            return "TimeRangeQuery{start=" + startTimestamp + ", end=" + endTimestamp + "}";
        }
    }

    /**
     * Key-value query - exact match on a field
     */
    public static class KeyValueQuery extends Query {
        private final String key;
        private final String value;

        public KeyValueQuery(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public boolean matches(Document doc) {
            String fieldValue = doc.getFieldValue(key);
            return value.equals(fieldValue);
        }

        @Override
        public String toString() {
            return "KeyValueQuery{" + key + "=" + value + "}";
        }
    }

    /**
     * Boolean query - combines multiple queries with AND/OR/NOT
     */
    public static class BooleanQuery extends Query {
        public enum Operator {
            AND, OR, NOT
        }

        private final Operator operator;
        private final List<Query> subQueries;

        private BooleanQuery(Operator operator, List<Query> subQueries) {
            this.operator = operator;
            this.subQueries = subQueries;
        }

        public static BooleanQuery and(Query... queries) {
            return new BooleanQuery(Operator.AND, List.of(queries));
        }

        public static BooleanQuery or(Query... queries) {
            return new BooleanQuery(Operator.OR, List.of(queries));
        }

        public static BooleanQuery not(Query query) {
            return new BooleanQuery(Operator.NOT, List.of(query));
        }

        public Operator getOperator() {
            return operator;
        }

        public List<Query> getSubQueries() {
            return subQueries;
        }

        public boolean matches(Document doc) {
            switch (operator) {
                case AND:
                    return subQueries.stream().allMatch(q -> q.matches(doc));
                case OR:
                    return subQueries.stream().anyMatch(q -> q.matches(doc));
                case NOT:
                    return !subQueries.get(0).matches(doc);
                default:
                    return false;
            }
        }

        @Override
        public String toString() {
            return "BooleanQuery{" + operator + ", subQueries=" + subQueries + "}";
        }
    }

    /**
     * Composite query - combines time range with field filters
     */
    public static class CompositeQuery extends Query {
        private final TimeRangeQuery timeRange;
        private final Query filterQuery; // Can be KeyValueQuery or BooleanQuery

        public CompositeQuery(TimeRangeQuery timeRange, Query filterQuery) {
            this.timeRange = timeRange;
            this.filterQuery = filterQuery;
        }

        public TimeRangeQuery getTimeRange() {
            return timeRange;
        }

        public Query getFilterQuery() {
            return filterQuery;
        }

        public boolean matches(Document doc) {
            return timeRange.matches(doc) && (filterQuery == null || filterQuery.matches(doc));
        }

        @Override
        public String toString() {
            return "CompositeQuery{timeRange=" + timeRange + ", filter=" + filterQuery + "}";
        }
    }

    /**
     * Abstract method to check if a document matches the query
     */
    public abstract boolean matches(Document doc);
}
