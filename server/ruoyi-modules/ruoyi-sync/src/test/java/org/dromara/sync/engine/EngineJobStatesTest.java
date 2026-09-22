package org.dromara.sync.engine;

import org.dromara.sync.domain.vo.EngineJobMetrics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("dev")
class EngineJobStatesTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    /**
     * Regression: Zeta 2.3.13 serializes every metric as a JSON string. The projection used
     * to accept numeric nodes only, so every counter came back null and the UI showed "-".
     */
    @Test
    void parsesStringTypedZetaMetrics() {
        EngineJobMetrics target = new EngineJobMetrics();
        EngineJobStates.applyMetrics(target, new SeaTunnelRestClient.JobSnapshot("1", "job", "RUNNING", null, mapper.readTree(
            "{\"SourceReceivedCount\":\"50\",\"SinkCommittedCount\":\"48\",\"SourceReceivedBytes\":\"5350\","
                + "\"SinkCommittedBytes\":\"5100\",\"SourceReceivedQPS\":\"0.37094\",\"SinkCommittedQPS\":\"0.35\"}")), "INCREMENTAL");
        assertEquals(50L, target.getSourceReceivedCount());
        assertEquals(48L, target.getSinkCommittedCount());
        assertEquals(5350L, target.getSourceReceivedBytes());
        assertEquals(0.37094, target.getSourceQps(), 1e-9);
        assertEquals(0.35, target.getSinkQps(), 1e-9);
        assertEquals(2L, target.getBacklogRows());
        assertNull(target.getCdcLagSeconds());
        assertEquals("CDC", target.getPhase());
    }

    @Test
    void numericNodesAndGarbageAreHandled() {
        EngineJobMetrics target = new EngineJobMetrics();
        EngineJobStates.applyMetrics(target, new SeaTunnelRestClient.JobSnapshot("1", "job", "INITIALIZING", null, mapper.readTree(
            "{\"SourceReceivedCount\":7,\"SinkCommittedCount\":\"n/a\",\"SourceReceivedQPS\":1.5,"
                + "\"SourceLatestEventTime\":\"1000\",\"SinkLatestCommitTime\":\"4000\"}")), "FULL_CDC");
        assertEquals(7L, target.getSourceReceivedCount());
        assertNull(target.getSinkCommittedCount());
        assertNull(target.getBacklogRows());
        assertEquals(1.5, target.getSourceQps(), 1e-9);
        assertEquals(3L, target.getCdcLagSeconds());
        assertEquals("SNAPSHOT", target.getPhase());
    }

    @Test
    void backlogNeverGoesNegative() {
        EngineJobMetrics target = new EngineJobMetrics();
        EngineJobStates.applyMetrics(target, new SeaTunnelRestClient.JobSnapshot("1", "job", "RUNNING", null, mapper.readTree(
            "{\"SourceReceivedCount\":\"10\",\"SinkCommittedCount\":\"12\"}")), "FULL_CDC");
        assertEquals(0L, target.getBacklogRows());
    }

    /** Zeta has no snapshot-complete signal, so the label never claims more than the mode + engine state allow. */
    @Test
    void phaseIsDerivedFromSyncModeAndNeverPretendsToKnowTheSnapshotBoundary() {
        assertEquals("SNAPSHOT", EngineJobStates.phaseOf("FULL", "RUNNING"));
        assertEquals("SNAPSHOT", EngineJobStates.phaseOf("FULL", "FINISHED"));
        assertEquals("CDC", EngineJobStates.phaseOf("INCREMENTAL", "INITIALIZING"));
        assertEquals("SNAPSHOT", EngineJobStates.phaseOf("FULL_CDC", "INITIALIZING"));
        assertEquals("MIXED", EngineJobStates.phaseOf("FULL_CDC", "RUNNING"));
        assertEquals("MIXED", EngineJobStates.phaseOf(null, "RUNNING"));
    }
}
