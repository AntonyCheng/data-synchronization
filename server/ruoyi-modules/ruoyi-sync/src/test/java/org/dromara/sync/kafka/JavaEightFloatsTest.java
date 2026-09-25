package org.dromara.sync.kafka;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Expected values are what {@code Double.parseDouble(Float.toString(f))} gives on Java 8 (1.8.0_342,
 * the engine's JVM, and 1.8.0_471 agree); the comments show what Java 19+ prints instead. The class
 * was checked against Java 8 for all 2^32 float bit patterns; these pin one float of each kind.
 */
@Tag("dev")
class JavaEightFloatsTest {

    @Test
    void ordinaryValuesArePrintedAlikeByEveryJava() {
        assertEquals(3.14159, JavaEightFloats.parsedValue(3.14159f));
        assertEquals(0.1, JavaEightFloats.parsedValue(0.1f));
        assertEquals(-1.5E-10, JavaEightFloats.parsedValue(-1.5E-10f));
        assertEquals(1234.567, JavaEightFloats.parsedValue(1234.567f));
        assertEquals(3.4E38, JavaEightFloats.parsedValue(3.4E38f));
        assertEquals(-0.0, JavaEightFloats.parsedValue(-0.0f));
        assertEquals(1.0, JavaEightFloats.parsedValue(1.0f));
    }

    @Test
    void integralFloatsKeepTheDigitsJavaEightPrinted() {
        assertEquals(2.2856919E9, JavaEightFloats.parsedValue(2285691904f));   // Java 19+: 2.285692E9
        assertEquals(7.0000008E7, JavaEightFloats.parsedValue(70000008f));     // Java 19+: 7.000001E7
        assertEquals(1.23456791E17, JavaEightFloats.parsedValue(1.23456789E17f)); // Java 19+: 1.2345679E17
        assertEquals(1.6777216E7, JavaEightFloats.parsedValue(16777217f));
    }

    @Test
    void freeFormatDigitsFollowJavaEightsRounding() {
        assertEquals(-5.3683995E25, JavaEightFloats.parsedValue(-5.3683995E25f)); // Java 19+: -5.3683996E25
        assertEquals(6.3387854E25, JavaEightFloats.parsedValue(6.3387855E25f));   // binary exponent 85
        assertEquals(1.26217745E-29, JavaEightFloats.parsedValue(1.2621775E-29f));
        assertEquals(1.17549435E-38, JavaEightFloats.parsedValue(Float.MIN_NORMAL)); // Java 19+: 1.1754944E-38
        assertEquals(2.24E-44, JavaEightFloats.parsedValue(2.2E-44f));             // subnormal
        assertEquals(1.4E-45, JavaEightFloats.parsedValue(Float.MIN_VALUE));
        assertEquals(3.4028235E38, JavaEightFloats.parsedValue(Float.MAX_VALUE));
    }
}
