package org.dromara.sync.kafka;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * The decimal Java 8's {@code Float.toString} prints for a float, as the double it parses back to.
 *
 * <p>Why this exists: the engine (SeaTunnel on Java 8) used to write a {@code FLOAT} column as
 * {@code Float.toString(value)} into the raw topic, and the bridge parsed that text as a double. That
 * double is what every published event carries for a {@code FLOAT}. The raw records the engine writes
 * now hold the float widened to a double instead, so the bridge has to print the float the way
 * Java 8 did to keep the published value. Java 19 replaced {@code Float.toString} with an algorithm
 * that always prints the shortest decimal (JDK-4511638). Java 8 prints more digits for integral
 * floats from 2^26 up (70000008 as {@code 7.0000008E7}, Java 19+: {@code 7.000001E7}) - about 9% of
 * all float bit patterns - and, for about 1.8 million others (mostly magnitudes around 1e25, and
 * some subnormals), a different last digit. The two digit-generation schemes below follow Java 8's published
 * behaviour: an integral float is printed as its integer with the insignificant low digits rounded
 * away; anything else is produced by Steele &amp; White's free-format digit generation with Java 8's
 * stopping and rounding rules. Verified against Java 8 for every one of the 2^32 float bit patterns.
 */
final class JavaEightFloats {

    private static final int SIGNIFICAND_BITS = 24;
    /** Scale of the significand as Java 8 handles it: aligned to a double's 52-bit fraction. */
    private static final int FRACTION_SHIFT = 52;
    private static final int[] SMALL_5_POW = new int[14];
    private static final long[] LONG_5_POW = new long[27];
    private static final int[] N_5_BITS = new int[27];

    static {
        long power = 1;
        for (int i = 0; i < LONG_5_POW.length; i++) {
            LONG_5_POW[i] = power;
            if (i < SMALL_5_POW.length) SMALL_5_POW[i] = (int) power;
            N_5_BITS[i] = 64 - Long.numberOfLeadingZeros(power);
            power *= 5;
        }
    }

    private JavaEightFloats() {
    }

    /** {@code Double.parseDouble(Float.toString(value))} as Java 8 computes it. Finite values only. */
    static double parsedValue(float value) {
        if (value == 0 || Float.isNaN(value) || Float.isInfinite(value)) return value;
        int bits = Float.floatToRawIntBits(value);
        int fraction = bits & 0x7FFFFF;
        int binaryExponent = (bits >>> 23) & 0xFF;
        int significantBits;
        if (binaryExponent == 0) {
            // Subnormal: normalise it.
            int leadingZeros = Integer.numberOfLeadingZeros(fraction);
            int shift = leadingZeros - (31 - 23);
            fraction <<= shift;
            binaryExponent = 1 - shift;
            significantBits = 32 - leadingZeros;
        } else {
            fraction |= 1 << 23;
            significantBits = SIGNIFICAND_BITS;
        }
        binaryExponent -= 127;
        Digits digits = digits(binaryExponent, ((long) fraction) << (FRACTION_SHIFT - 23), significantBits);
        double magnitude = new BigDecimal(new BigInteger(digits.text()))
            .scaleByPowerOfTen(digits.decimalExponent() - digits.text().length()).doubleValue();
        return value < 0 ? -magnitude : magnitude;
    }

    /** The digits d1..dn and exponent e of the printed value 0.d1..dn x 10^e. */
    private record Digits(String text, int decimalExponent) {
    }

    private static Digits digits(int binaryExponent, long fractionBits, int significantBits) {
        int tailZeros = Long.numberOfTrailingZeros(fractionBits);
        int fractionBitCount = FRACTION_SHIFT + 1 - tailZeros;
        // Bits of the value right of the binary point.
        int tinyBits = Math.max(0, fractionBitCount - binaryExponent - 1);
        if (binaryExponent <= 62 && binaryExponent >= -21 && tinyBits == 0
            && fractionBitCount + N_5_BITS[0] < 64) {
            // An integer: print it, rounding away the digits below the float's precision.
            int insignificant = binaryExponent > significantBits
                ? insignificantDigits(binaryExponent - significantBits - 1) : 0;
            long integer = binaryExponent >= FRACTION_SHIFT
                ? fractionBits << (binaryExponent - FRACTION_SHIFT)
                : fractionBits >>> (FRACTION_SHIFT - binaryExponent);
            return integerDigits(integer, insignificant);
        }
        return freeFormatDigits(binaryExponent, fractionBits, significantBits, tailZeros, fractionBitCount, tinyBits);
    }

    /** Low decimal digits of an integral float below its precision: floor(p * log10 2) for 1 &lt; p &lt; 64. */
    private static int insignificantDigits(int powerOfTwo) {
        if (powerOfTwo <= 1 || powerOfTwo >= 64) return 0;
        return (int) Math.floor(powerOfTwo * Math.log10(2));
    }

    private static Digits integerDigits(long value, int insignificant) {
        int exponent = 0;
        if (insignificant != 0) {
            long scale = LONG_5_POW[insignificant] << insignificant;
            long residue = value % scale;
            value /= scale;
            exponent += insignificant;
            if (residue >= (scale >> 1)) value++;
        }
        String text = Long.toString(value);
        String trimmed = text.replaceFirst("0+$", "");
        exponent += text.length() - trimmed.length();
        return new Digits(trimmed, exponent + trimmed.length());
    }

    /**
     * Steele &amp; White free-format generation: value = B / S x 10^decExp with 1 &lt;= B / S &lt; 10,
     * M = half an ULP scaled like B; digits are produced until the remainder is within M of either end.
     */
    private static Digits freeFormatDigits(int binaryExponent, long fractionBits, int significantBits,
                                           int tailZeros, int fractionBitCount, int tinyBits) {
        int decExp = estimateDecimalExponent(fractionBits, binaryExponent);
        int b5 = Math.max(0, -decExp);
        int b2 = b5 + tinyBits + binaryExponent;
        int s5 = Math.max(0, decExp);
        int s2 = s5 + tinyBits;
        int m5 = b5;
        int m2 = b2 - significantBits;

        fractionBits >>>= tailZeros;
        b2 -= fractionBitCount - 1;
        int common2 = Math.min(b2, s2);
        b2 -= common2;
        s2 -= common2;
        m2 -= common2;
        // Below an exact power of two the next float is only half as far away.
        if (fractionBitCount == 1) m2 -= 1;
        if (m2 < 0) {
            b2 -= m2;
            s2 -= m2;
            m2 = 0;
        }

        StringBuilder digits = new StringBuilder(12);
        boolean low;
        boolean high;
        long lowDigitDifference;
        int bBits = fractionBitCount + b2 + (b5 < N_5_BITS.length ? N_5_BITS[b5] : b5 * 3);
        int tenSBits = s2 + 1 + (s5 + 1 < N_5_BITS.length ? N_5_BITS[s5 + 1] : (s5 + 1) * 3);
        if (bBits < 64 && tenSBits < 64) {
            if (bBits < 32 && tenSBits < 32) {
                int b = ((int) fractionBits * SMALL_5_POW[b5]) << b2;
                int s = SMALL_5_POW[s5] << s2;
                int m = SMALL_5_POW[m5] << m2;
                int tens = s * 10;
                int q = b / s;
                b = 10 * (b % s);
                m *= 10;
                low = b < m;
                high = b + m > tens;
                if (q == 0 && !high) decExp--;
                else digits.append((char) ('0' + q));
                if (decExp < -3 || decExp >= 8) high = low = false;
                while (!low && !high) {
                    q = b / s;
                    b = 10 * (b % s);
                    m *= 10;
                    if (m > 0L) {
                        low = b < m;
                        high = b + m > tens;
                    } else {
                        low = true;
                        high = true;
                    }
                    digits.append((char) ('0' + q));
                }
                lowDigitDifference = (b << 1) - tens;
            } else {
                long b = (fractionBits * LONG_5_POW[b5]) << b2;
                long s = LONG_5_POW[s5] << s2;
                long m = LONG_5_POW[m5] << m2;
                long tens = s * 10L;
                int q = (int) (b / s);
                b = 10L * (b % s);
                m *= 10L;
                low = b < m;
                high = b + m > tens;
                if (q == 0 && !high) decExp--;
                else digits.append((char) ('0' + q));
                if (decExp < -3 || decExp >= 8) high = low = false;
                while (!low && !high) {
                    q = (int) (b / s);
                    b = 10 * (b % s);
                    m *= 10;
                    if (m > 0L) {
                        low = b < m;
                        high = b + m > tens;
                    } else {
                        low = true;
                        high = true;
                    }
                    digits.append((char) ('0' + q));
                }
                lowDigitDifference = (b << 1) - tens;
            }
        } else {
            BigInteger s = BigInteger.valueOf(5).pow(s5).shiftLeft(s2);
            BigInteger b = BigInteger.valueOf(fractionBits).multiply(BigInteger.valueOf(5).pow(b5)).shiftLeft(b2);
            BigInteger m = BigInteger.valueOf(5).pow(m5 + 1).shiftLeft(m2 + 1);
            BigInteger tenS = BigInteger.valueOf(5).pow(s5 + 1).shiftLeft(s2 + 1);
            BigInteger[] quotient = b.divideAndRemainder(s);
            int q = quotient[0].intValueExact();
            b = quotient[1].multiply(BigInteger.TEN);
            low = b.compareTo(m) < 0;
            high = tenS.compareTo(b.add(m)) <= 0;
            if (q == 0 && !high) decExp--;
            else digits.append((char) ('0' + q));
            if (decExp < -3 || decExp >= 8) high = low = false;
            while (!low && !high) {
                quotient = b.divideAndRemainder(s);
                q = quotient[0].intValueExact();
                b = quotient[1].multiply(BigInteger.TEN);
                m = m.multiply(BigInteger.TEN);
                low = b.compareTo(m) < 0;
                high = tenS.compareTo(b.add(m)) <= 0;
                digits.append((char) ('0' + q));
            }
            lowDigitDifference = high && low ? b.shiftLeft(1).compareTo(tenS) : 0L;
        }
        int decimalExponent = decExp + 1;
        if (high) {
            if (low) {
                if (lowDigitDifference == 0L) {
                    if ((digits.charAt(digits.length() - 1) & 1) != 0) decimalExponent += roundUp(digits);
                } else if (lowDigitDifference > 0) {
                    decimalExponent += roundUp(digits);
                }
            } else {
                decimalExponent += roundUp(digits);
            }
        }
        return new Digits(digits.toString(), decimalExponent);
    }

    /** Adds one to the last digit; returns 1 when that carries out of the first digit (all 9s). */
    private static int roundUp(StringBuilder digits) {
        int i = digits.length() - 1;
        char q = digits.charAt(i);
        if (q == '9') {
            while (q == '9' && i > 0) {
                digits.setCharAt(i, '0');
                q = digits.charAt(--i);
            }
            if (q == '9') {
                digits.setCharAt(0, '1');
                return 1;
            }
        }
        digits.setCharAt(i, (char) (q + 1));
        return 0;
    }

    /** floor(log10(value)), estimated from the binary exponent and significand. */
    private static int estimateDecimalExponent(long fractionBits, int binaryExponent) {
        double d2 = Double.longBitsToDouble(0x3FF0000000000000L | (fractionBits & 0x000FFFFFFFFFFFFFL));
        double d = (d2 - 1.5D) * 0.289529654D + 0.176091259 + (double) binaryExponent * 0.301029995663981;
        return (int) Math.floor(d);
    }
}
