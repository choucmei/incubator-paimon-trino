package org.apache.paimon.trino;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static java.lang.String.format;

public class TimeTest {
    public static void main(String[] args) {
        int precision = 0;
        long picos = 37790506000000000l;
        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        if (rescale(rescale(picos, 12, precision), precision, 12) != picos) {
            throw new IllegalArgumentException(format("picos contains data beyond specified precision (%s): %s", precision, picos));
        }
        if (picos < 0 || picos >= PICOSECONDS_PER_DAY) {
            throw new IllegalArgumentException("picos is out of range: " + picos);
        }
    }

    static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }

        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value /= scaleFactor(toPrecision, fromPrecision);
        }

        return value;
    }

    private static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }

        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L
    };

}
