package com.logingestion.storage;

import com.logingestion.index.InvertedIndex;
import com.logingestion.index.TimeBasedIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages periodic snapshots of in-memory data structures
 * Snapshots are used for faster recovery than replaying WAL
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
    private static final String SNAPSHOT_PREFIX = "snapshot_";
    private static final String TIME_INDEX_SUFFIX = "_timeindex.dat";
    private static final String INVERTED_INDEX_SUFFIX = "_invertedindex.dat";

    private final Path snapshotPath;
    private final Lock snapshotLock;

    public SnapshotManager(String snapshotDirectory) throws IOException {
        this.snapshotPath = Paths.get(snapshotDirectory);
        this.snapshotLock = new ReentrantLock();

        // Create snapshot directory if it doesn't exist
        Files.createDirectories(snapshotPath);

        logger.info("SnapshotManager initialized at {}", snapshotPath);
    }

    /**
     * Create a snapshot of the current state
     */
    public void createSnapshot(TimeBasedIndex timeIndex, InvertedIndex invertedIndex)
            throws IOException {
        snapshotLock.lock();
        try {
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String snapshotId = SNAPSHOT_PREFIX + timestamp;

            Path timeIndexFile = snapshotPath.resolve(snapshotId + TIME_INDEX_SUFFIX);
            Path invertedIndexFile = snapshotPath.resolve(snapshotId + INVERTED_INDEX_SUFFIX);

            logger.info("Creating snapshot: {}", snapshotId);

            // Serialize time-based index
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new BufferedOutputStream(new FileOutputStream(timeIndexFile.toFile())))) {
                oos.writeObject(timeIndex);
            }

            // Serialize inverted index
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new BufferedOutputStream(new FileOutputStream(invertedIndexFile.toFile())))) {
                oos.writeObject(invertedIndex);
            }

            // Delete old snapshots, keep only the latest
            deleteOldSnapshots(snapshotId);

            logger.info("Snapshot created successfully: {}", snapshotId);
        } finally {
            snapshotLock.unlock();
        }
    }

    /**
     * Load the latest snapshot
     */
    public SnapshotData loadLatestSnapshot() throws IOException, ClassNotFoundException {
        snapshotLock.lock();
        try {
            String latestSnapshot = findLatestSnapshot();

            if (latestSnapshot == null) {
                logger.info("No snapshot found");
                return null;
            }

            logger.info("Loading snapshot: {}", latestSnapshot);

            Path timeIndexFile = snapshotPath.resolve(latestSnapshot + TIME_INDEX_SUFFIX);
            Path invertedIndexFile = snapshotPath.resolve(latestSnapshot + INVERTED_INDEX_SUFFIX);

            TimeBasedIndex timeIndex;
            InvertedIndex invertedIndex;

            // Deserialize time-based index
            try (ObjectInputStream ois = new ObjectInputStream(
                    new BufferedInputStream(new FileInputStream(timeIndexFile.toFile())))) {
                timeIndex = (TimeBasedIndex) ois.readObject();
            }

            // Deserialize inverted index
            try (ObjectInputStream ois = new ObjectInputStream(
                    new BufferedInputStream(new FileInputStream(invertedIndexFile.toFile())))) {
                invertedIndex = (InvertedIndex) ois.readObject();
            }

            logger.info("Snapshot loaded successfully: {}", latestSnapshot);
            return new SnapshotData(timeIndex, invertedIndex);
        } finally {
            snapshotLock.unlock();
        }
    }

    /**
     * Find the latest snapshot by timestamp
     */
    private String findLatestSnapshot() throws IOException {
        File[] files = snapshotPath.toFile().listFiles(
                (dir, name) -> name.startsWith(SNAPSHOT_PREFIX) && name.endsWith(TIME_INDEX_SUFFIX));

        if (files == null || files.length == 0) {
            return null;
        }

        // Find the most recent snapshot
        String latest = null;
        for (File file : files) {
            String name = file.getName();
            String snapshotId = name.substring(0, name.lastIndexOf('_'));
            if (latest == null || snapshotId.compareTo(latest) > 0) {
                latest = snapshotId;
            }
        }

        return latest;
    }

    /**
     * Delete old snapshots, keeping only the latest
     */
    private void deleteOldSnapshots(String currentSnapshot) throws IOException {
        File[] files = snapshotPath.toFile().listFiles(
                (dir, name) -> name.startsWith(SNAPSHOT_PREFIX));

        if (files == null) {
            return;
        }

        int deletedCount = 0;
        for (File file : files) {
            if (!file.getName().startsWith(currentSnapshot)) {
                if (file.delete()) {
                    deletedCount++;
                }
            }
        }

        if (deletedCount > 0) {
            logger.info("Deleted {} old snapshot files", deletedCount);
        }
    }

    /**
     * Delete all snapshots
     */
    public void deleteAllSnapshots() throws IOException {
        snapshotLock.lock();
        try {
            File[] files = snapshotPath.toFile().listFiles(
                    (dir, name) -> name.startsWith(SNAPSHOT_PREFIX));

            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
                logger.info("Deleted all snapshots");
            }
        } finally {
            snapshotLock.unlock();
        }
    }

    /**
     * Container for snapshot data
     */
    public static class SnapshotData {
        private final TimeBasedIndex timeIndex;
        private final InvertedIndex invertedIndex;

        public SnapshotData(TimeBasedIndex timeIndex, InvertedIndex invertedIndex) {
            this.timeIndex = timeIndex;
            this.invertedIndex = invertedIndex;
        }

        public TimeBasedIndex getTimeIndex() {
            return timeIndex;
        }

        public InvertedIndex getInvertedIndex() {
            return invertedIndex;
        }
    }
}
