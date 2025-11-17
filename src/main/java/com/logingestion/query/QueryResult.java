package com.logingestion.query;

import com.logingestion.model.Document;

import java.util.List;

/**
 * Result of a query execution
 */
public class QueryResult {
    private final List<Document> documents;
    private final long executionTimeMs;
    private final String errorMessage;
    private final boolean success;

    public QueryResult(List<Document> documents, long executionTimeMs) {
        this.documents = documents;
        this.executionTimeMs = executionTimeMs;
        this.errorMessage = null;
        this.success = true;
    }

    public QueryResult(List<Document> documents, long executionTimeMs, String errorMessage) {
        this.documents = documents;
        this.executionTimeMs = executionTimeMs;
        this.errorMessage = errorMessage;
        this.success = false;
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getDocumentCount() {
        return documents.size();
    }

    @Override
    public String toString() {
        if (success) {
            return String.format("QueryResult{documents=%d, time=%dms}",
                    documents.size(), executionTimeMs);
        } else {
            return String.format("QueryResult{error='%s', time=%dms}",
                    errorMessage, executionTimeMs);
        }
    }
}
