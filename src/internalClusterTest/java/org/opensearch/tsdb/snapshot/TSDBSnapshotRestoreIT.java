/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.snapshot;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.opensearch.tsdb.utils.TSDBTestUtils.getSampleCountViaAggregation;

/**
 * Integration test for TSDB snapshot and restore functionality.
 *
 * <p>Snapshots only capture data from closed chunk indexes (CCI), not the head. This test
 * ensures all ingested data is flushed from head into CCI before taking a snapshot.
 */
public class TSDBSnapshotRestoreIT extends TimeSeriesTestFramework {

    private static final String INDEX_NAME = "tsdb_snapshot_test";
    private static final String REPO_NAME = "test-repo";
    private static final String SNAPSHOT_NAME = "test-snapshot";

    private static final int NUM_SERIES = 10;
    private static final int SAMPLES_PER_SERIES = 50;

    private static final long BASE_TIMESTAMP_MS = System.currentTimeMillis() - 2 * 3_600_000L; // 2 hours ago
    private static final long SAMPLE_INTERVAL_MS = 10_000L; // 10s between samples

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TSDBPlugin.class);
    }

    @Override
    public Map<String, Object> getDefaultIndexSettings() {
        return snapshotTestSettings();
    }

    /**
     * Returns index settings tuned for deterministic snapshot testing
     */
    private Map<String, Object> snapshotTestSettings() {
        Map<String, Object> settings = new java.util.HashMap<>();
        settings.put("index.tsdb_engine.enabled", true);
        settings.put("index.tsdb_engine.labels.storage_type", "binary");
        settings.put("index.tsdb_engine.lang.m3.default_step_size", "10s");
        settings.put("index.store.factory", "tsdb_store");
        settings.put("index.refresh_interval", "1s");
        settings.put("index.queries.cache.enabled", false);
        settings.put("index.requests.cache.enable", false);
        settings.put("index.translog.read_forward", true);
        // ooo_cutoff=10m: accept samples within 10 minutes of maxTime. After the heartbeat at now,
        // cutoffTimestamp = now - 10m, so all 2h-old data is past the cutoff and closeable on flush.
        settings.put("index.tsdb_engine.ooo_cutoff", "10m");
        // 100%: disables the rate limiter so a single force flush closes all eligible chunks
        settings.put("index.tsdb_engine.max_closeable_chunks_per_chunk_range_percentage", 100);
        return settings;
    }

    @Override
    public Map<String, Object> getDefaultIndexMapping() {
        try {
            return parseMappingFromConstants();
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse mapping from constants", e);
        }
    }

    public void testBasicSnapshotAndRestore() throws Exception {
        internalCluster().startNodes(1);
        ensureStableCluster(1);

        // Create TSDB index with a single shard and no replicas for simplicity.
        // snapshotTestSettings() sets ooo_cutoff=0s and rate-limit=100% so one force flush
        // moves all chunks from head to CCI.
        IndexConfig indexConfig = new IndexConfig(INDEX_NAME, 1, 0, snapshotTestSettings(), parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        // Ingest data with timestamps from 2h ago
        List<TimeSeriesSample> samples = generateSamples(NUM_SERIES, SAMPLES_PER_SERIES, BASE_TIMESTAMP_MS, SAMPLE_INTERVAL_MS);
        ingestSamples(samples, INDEX_NAME);

        // Ingest one heartbeat sample at current time per series to push maxTime to now.
        // With ooo_cutoff="10m", cutoffTimestamp becomes now - 10m, which is well after the
        // 2h-old data — making all those chunks eligible to be closed on force flush.
        long nowMs = System.currentTimeMillis();
        List<TimeSeriesSample> heartbeats = new ArrayList<>();
        for (int seriesIdx = 0; seriesIdx < NUM_SERIES; seriesIdx++) {
            Map<String, String> labels = Map.of("__name__", "test_metric", "series_id", String.valueOf(seriesIdx), "env", "test");
            heartbeats.add(new TimeSeriesSample(Instant.ofEpochMilli(nowMs), 0.0, labels));
        }
        ingestSamples(heartbeats, INDEX_NAME);

        // Force flush to create CCIs
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Record sample count before taking snapshot
        long startTime = BASE_TIMESTAMP_MS - SAMPLE_INTERVAL_MS;
        long endTime = BASE_TIMESTAMP_MS + (SAMPLES_PER_SERIES * SAMPLE_INTERVAL_MS) + SAMPLE_INTERVAL_MS;
        int samplesBeforeSnapshot = getSampleCountViaAggregation(client(), INDEX_NAME, startTime, endTime, SAMPLE_INTERVAL_MS);

        logger.info("Found {} samples before snapshot", samplesBeforeSnapshot);
        assertThat("Expected samples before snapshot", samplesBeforeSnapshot, equalTo(NUM_SERIES * SAMPLES_PER_SERIES));

        // Create a filesystem snapshot repository
        Path repoPath = randomRepoPath();
        client().admin()
            .cluster()
            .preparePutRepository(REPO_NAME)
            .setType("fs")
            .setSettings(org.opensearch.common.settings.Settings.builder().put("location", repoPath).put("compress", randomBoolean()))
            .get();

        // Take the snapshot
        logger.info("Creating snapshot");
        CreateSnapshotResponse createResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(REPO_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();

        SnapshotInfo snapshotInfo = createResponse.getSnapshotInfo();
        assertThat("Snapshot should succeed", snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat("Snapshot should have successful shards", snapshotInfo.successfulShards(), greaterThan(0));
        assertThat("All shards should succeed", snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        logger.info("Snapshot created: {}", snapshotInfo);

        // Delete the original index
        logger.info("Deleting original index");
        client().admin().indices().prepareDelete(INDEX_NAME).get();
        assertBusy(() -> assertFalse("Index should be deleted", client().admin().indices().prepareExists(INDEX_NAME).get().isExists()));

        // Restore from snapshot
        logger.info("Restoring from snapshot");
        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();

        assertThat("Restore response should have shards", restoreResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen(INDEX_NAME);

        // Validate sample count after restore matches what was there before
        int samplesAfterRestore = getSampleCountViaAggregation(client(), INDEX_NAME, startTime, endTime, SAMPLE_INTERVAL_MS);
        logger.info("Found {} samples after restore", samplesAfterRestore);
        assertThat("Sample count after restore should match pre-snapshot count", samplesAfterRestore, equalTo(samplesBeforeSnapshot));
    }

    private List<TimeSeriesSample> generateSamples(int numSeries, int samplesPerSeries, long baseTimestampMs, long intervalMs) {
        List<TimeSeriesSample> samples = new ArrayList<>();
        for (int seriesIdx = 0; seriesIdx < numSeries; seriesIdx++) {
            Map<String, String> labels = Map.of("__name__", "test_metric", "series_id", String.valueOf(seriesIdx), "env", "test");
            for (int sampleIdx = 0; sampleIdx < samplesPerSeries; sampleIdx++) {
                long timestamp = baseTimestampMs + (sampleIdx * intervalMs);
                double value = seriesIdx * 1000.0 + sampleIdx;
                samples.add(new TimeSeriesSample(Instant.ofEpochMilli(timestamp), value, labels));
            }
        }
        logger.info("Generated {} samples for {} time series", samples.size(), numSeries);
        return samples;
    }
}
