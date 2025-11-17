package com.logingestion.model;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Converts ISO8601 timestamps to epoch milliseconds
 * Supports optional baseline offset for relative timestamps
 */
public class TimestampConverter {
    private final long baselineMillis;
    private final boolean useRelativeTime;

    /**
     * Create converter with absolute epoch time (standard)
     */
    public TimestampConverter() {
        this.baselineMillis = 0;
        this.useRelativeTime = false;
    }

    /**
     * Create converter with relative time from baseline
     * @param baselineDate ISO8601 baseline date (e.g., "2020-12-01T00:00:00Z")
     */
    public TimestampConverter(String baselineDate) {
        this.baselineMillis = parseISO8601(baselineDate);
        this.useRelativeTime = true;
    }

    /**
     * Parse ISO8601 string to epoch milliseconds
     * Supports various formats:
     * - 2024-12-05T10:30:45.123Z (millisecond precision)
     * - 2024-12-05T10:30:45Z (second precision)
     * - 2024-12-05T10:30:45+05:30 (with timezone)
     */
    public long parseISO8601(String iso8601) {
        if (iso8601 == null || iso8601.isEmpty()) {
            throw new IllegalArgumentException("Timestamp cannot be null or empty");
        }

        try {
            // Try parsing as ZonedDateTime (handles all formats)
            ZonedDateTime zdt = ZonedDateTime.parse(iso8601, DateTimeFormatter.ISO_DATE_TIME);
            long absoluteMillis = zdt.toInstant().toEpochMilli();

            // Return relative or absolute based on configuration
            return useRelativeTime ? (absoluteMillis - baselineMillis) : absoluteMillis;

        } catch (DateTimeParseException e) {
            // Try parsing as Instant (simpler formats)
            try {
                Instant instant = Instant.parse(iso8601);
                long absoluteMillis = instant.toEpochMilli();
                return useRelativeTime ? (absoluteMillis - baselineMillis) : absoluteMillis;
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException(
                    "Invalid ISO8601 timestamp: " + iso8601 +
                    ". Expected format like: 2024-12-05T10:30:45.123Z", e);
            }
        }
    }

    /**
     * Convert back to ISO8601 string
     */
    public String toISO8601(long timestamp) {
        long absoluteMillis = useRelativeTime ? (timestamp + baselineMillis) : timestamp;
        return Instant.ofEpochMilli(absoluteMillis).toString();
    }

    /**
     * Get the baseline epoch milliseconds
     */
    public long getBaselineMillis() {
        return baselineMillis;
    }

    /**
     * Check if using relative time
     */
    public boolean isUsingRelativeTime() {
        return useRelativeTime;
    }

    /**
     * Validate that a timestamp is within reasonable bounds
     */
    public boolean isValid(long timestamp) {
        if (useRelativeTime) {
            // Relative timestamps should be non-negative and not too far in future
            long maxOffset = 100L * 365 * 24 * 60 * 60 * 1000; // 100 years
            return timestamp >= 0 && timestamp <= maxOffset;
        } else {
            // Absolute timestamps should be reasonable (between 2000 and 2100)
            long year2000 = 946684800000L;  // 2000-01-01
            long year2100 = 4102444800000L; // 2100-01-01
            return timestamp >= year2000 && timestamp <= year2100;
        }
    }
}
