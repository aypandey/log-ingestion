package com.logingestion.model;

import com.google.gson.Gson;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Configuration for the log ingestion system
 */
public class IndexConfig {
    private int hotTierRetentionDays;
    private int hotTierSnapshotIntervalMinutes;
    private String coldStoragePath;
    private String walPath;
    private String snapshotPath;
    private List<String> indexedFields;
    private int maxHotTierMemoryMB;
    private int compactionIntervalHours;
    private String timestampField;
    private String timestampFormat;
    private String timestampBaseline;

    public static IndexConfig loadFromFile(String configPath) throws IOException {
        try (FileReader reader = new FileReader(configPath)) {
            return new Gson().fromJson(reader, IndexConfig.class);
        }
    }

    public static IndexConfig getDefault() {
        IndexConfig config = new IndexConfig();
        config.hotTierRetentionDays = 15;
        config.hotTierSnapshotIntervalMinutes = 30;
        config.coldStoragePath = "./cold-storage";
        config.walPath = "./wal";
        config.snapshotPath = "./snapshots";
        config.indexedFields = List.of("level", "service", "userId", "message");
        config.maxHotTierMemoryMB = 1024;
        config.compactionIntervalHours = 6;
        config.timestampField = "creationTimestamp";
        config.timestampFormat = "EPOCH_MILLIS"; // or "ISO8601"
        config.timestampBaseline = null; // null means absolute epoch time
        return config;
    }

    // Getters
    public int getHotTierRetentionDays() {
        return hotTierRetentionDays;
    }

    public long getHotTierRetentionMillis() {
        return hotTierRetentionDays * 24L * 60 * 60 * 1000;
    }

    public int getHotTierSnapshotIntervalMinutes() {
        return hotTierSnapshotIntervalMinutes;
    }

    public String getColdStoragePath() {
        return coldStoragePath;
    }

    public String getWalPath() {
        return walPath;
    }

    public String getSnapshotPath() {
        return snapshotPath;
    }

    public List<String> getIndexedFields() {
        return indexedFields;
    }

    public int getMaxHotTierMemoryMB() {
        return maxHotTierMemoryMB;
    }

    public int getCompactionIntervalHours() {
        return compactionIntervalHours;
    }

    // Setters
    public void setHotTierRetentionDays(int hotTierRetentionDays) {
        this.hotTierRetentionDays = hotTierRetentionDays;
    }

    public void setHotTierSnapshotIntervalMinutes(int hotTierSnapshotIntervalMinutes) {
        this.hotTierSnapshotIntervalMinutes = hotTierSnapshotIntervalMinutes;
    }

    public void setColdStoragePath(String coldStoragePath) {
        this.coldStoragePath = coldStoragePath;
    }

    public void setWalPath(String walPath) {
        this.walPath = walPath;
    }

    public void setSnapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
    }

    public void setIndexedFields(List<String> indexedFields) {
        this.indexedFields = indexedFields;
    }

    public void setMaxHotTierMemoryMB(int maxHotTierMemoryMB) {
        this.maxHotTierMemoryMB = maxHotTierMemoryMB;
    }

    public void setCompactionIntervalHours(int compactionIntervalHours) {
        this.compactionIntervalHours = compactionIntervalHours;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public String getTimestampBaseline() {
        return timestampBaseline;
    }

    public void setTimestampBaseline(String timestampBaseline) {
        this.timestampBaseline = timestampBaseline;
    }
}
