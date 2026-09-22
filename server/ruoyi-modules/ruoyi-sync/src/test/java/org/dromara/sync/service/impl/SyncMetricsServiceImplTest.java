package org.dromara.sync.service.impl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class SyncMetricsServiceImplTest {

    @Test
    void shortSeriesIsReturnedUntouched() {
        List<Integer> points = List.of(1, 2, 3);
        assertSame(points, SyncMetricsServiceImpl.thin(points, 10));
        assertSame(points, SyncMetricsServiceImpl.thin(points, 1));
    }

    /** A 24 h window at the 30 s cadence is 2 880 points; the chart cap must keep the first and the last. */
    @Test
    void longSeriesIsThinnedEvenlyAndKeepsBothEnds() {
        List<Integer> points = IntStream.range(0, 2880).boxed().toList();
        List<Integer> thinned = SyncMetricsServiceImpl.thin(points, 1500);
        assertTrue(thinned.size() <= 1501, "size=" + thinned.size());
        assertEquals(0, thinned.get(0));
        assertEquals(2879, thinned.get(thinned.size() - 1));
        // Stride is uniform: every kept point is a multiple of the stride except the appended last one.
        int stride = thinned.get(1) - thinned.get(0);
        for (int index = 1; index < thinned.size() - 1; index++) {
            assertEquals(stride, thinned.get(index) - thinned.get(index - 1));
        }
    }
}
