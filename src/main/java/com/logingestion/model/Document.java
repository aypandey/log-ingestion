package com.logingestion.model;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a log document with timestamp and JSON content
 */
public class Document implements Serializable, Comparable<Document> {
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new Gson();

    private final String id;
    private final long creationTimestamp; // milliseconds since epoch (or relative to baseline)
    private final String jsonContent;
    private transient JsonObject parsedJson;

    /**
     * Create document with default timestamp parsing (epoch millis)
     */
    public Document(String jsonContent) {
        this(jsonContent, null);
    }

    /**
     * Create document with custom timestamp converter
     */
    public Document(String jsonContent, TimestampConverter timestampConverter) {
        this.id = UUID.randomUUID().toString();
        this.jsonContent = jsonContent;
        this.parsedJson = JsonParser.parseString(jsonContent).getAsJsonObject();

        // Extract creationTimestamp from JSON
        if (!parsedJson.has("creationTimestamp")) {
            throw new IllegalArgumentException("Document must contain 'creationTimestamp' field");
        }

        // Parse timestamp based on format
        if (timestampConverter != null) {
            // Use converter for ISO8601 or custom formats
            String timestampStr = parsedJson.get("creationTimestamp").getAsString();
            this.creationTimestamp = timestampConverter.parseISO8601(timestampStr);
        } else {
            // Default: assume epoch milliseconds (long)
            this.creationTimestamp = parsedJson.get("creationTimestamp").getAsLong();
        }
    }

    public Document(String id, long creationTimestamp, String jsonContent) {
        this.id = id;
        this.creationTimestamp = creationTimestamp;
        this.jsonContent = jsonContent;
        this.parsedJson = JsonParser.parseString(jsonContent).getAsJsonObject();
    }

    public String getId() {
        return id;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public String getJsonContent() {
        return jsonContent;
    }

    public JsonObject getParsedJson() {
        if (parsedJson == null) {
            parsedJson = JsonParser.parseString(jsonContent).getAsJsonObject();
        }
        return parsedJson;
    }

    /**
     * Get value for a specific field from the JSON document
     */
    public String getFieldValue(String fieldName) {
        JsonObject json = getParsedJson();
        if (json.has(fieldName) && !json.get(fieldName).isJsonNull()) {
            return json.get(fieldName).getAsString();
        }
        return null;
    }

    @Override
    public int compareTo(Document other) {
        return Long.compare(this.creationTimestamp, other.creationTimestamp);
    }

    @Override
    public String toString() {
        return "Document{id='" + id + "', timestamp=" + creationTimestamp +
               ", content=" + jsonContent + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return id.equals(document.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
