package org.dromara.sync.support;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("dev")
class GroupStatusesTest {

    @Test
    void aggregatesItemStatesIntoGroupState() {
        assertEquals("DEGRADED", GroupStatuses.aggregate(List.of("RUNNING", "DDL_BLOCKED")));
        assertEquals("DEGRADED", GroupStatuses.aggregate(List.of("RUNNING", "FAILED", "STOPPED")));
        assertEquals("FAILED", GroupStatuses.aggregate(List.of("FAILED", "STOPPED")));
        assertEquals("RUNNING", GroupStatuses.aggregate(List.of("RUNNING", "PAUSED")));
        assertEquals("PAUSING", GroupStatuses.aggregate(List.of("PAUSING", "PAUSED")));
        assertEquals("PAUSED", GroupStatuses.aggregate(List.of("PAUSED", "PAUSED")));
        assertEquals("STOPPED", GroupStatuses.aggregate(List.of("STOPPED", "FINISHED")));
        assertEquals("DRAFT", GroupStatuses.aggregate(List.of("PENDING")));
        assertEquals("DRAFT", GroupStatuses.aggregate(List.of()));
        // A null item status (never expected, but persisted rows are outside our control) must not blow up.
        assertEquals("RUNNING", GroupStatuses.aggregate(Arrays.asList("RUNNING", null)));
    }
}
