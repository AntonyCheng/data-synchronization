package org.dromara.sync.service.impl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for the KEY_RANGE block-count guard: block SIZE was bounded but block
 * COUNT (span / step) was not, letting a small blockSize on a wide key range turn a
 * "bounded, read-only" check into an effectively unbounded number of serial JDBC
 * round-trips inside the request thread.
 */
@Tag("dev")
class DataConsistencyServiceImplTest {

    @Test
    void computesACeilingEstimateOfTheBlockCount() {
        // 0..999 with step 100 needs 10 real iterations; the estimate is deliberately
        // conservative (see its Javadoc) and may overshoot by one - 11 here - which only
        // makes the cap check stricter, never looser.
        BigDecimal blocks = DataConsistencyServiceImpl.estimateBlockCount(
            BigDecimal.ZERO, BigDecimal.valueOf(999), BigDecimal.valueOf(100));
        assertEquals(BigDecimal.valueOf(11), blocks);
    }

    @Test
    void smallBlockSizeOnAWideRangeExceedsTheCap() {
        BigDecimal blocks = DataConsistencyServiceImpl.estimateBlockCount(
            BigDecimal.ZERO, BigDecimal.valueOf(100_000_000L), BigDecimal.ONE);
        assertTrue(blocks.compareTo(BigDecimal.valueOf(DataConsistencyServiceImpl.MAX_KEY_RANGE_BLOCKS)) > 0,
            "a range of 100M with step 1 must exceed the per-check block cap");
    }

    @Test
    void aSuitablyLargeBlockSizeStaysWithinTheCap() {
        BigDecimal blocks = DataConsistencyServiceImpl.estimateBlockCount(
            BigDecimal.ZERO, BigDecimal.valueOf(100_000_000L), BigDecimal.valueOf(1_000_000));
        assertTrue(blocks.compareTo(BigDecimal.valueOf(DataConsistencyServiceImpl.MAX_KEY_RANGE_BLOCKS)) <= 0,
            "a range of 100M with step 1,000,000 must fit within the per-check block cap");
    }
}
