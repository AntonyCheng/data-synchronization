import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.util.Arrays;

/**
 * Guards the GoldenDB Table_map patch in {@code TableMapEventDataDeserializer}.
 *
 * The GoldenDB cases are raw event bodies captured from a live DN node
 * (10.9.0.78:8880 -> 192.168.100.17:5502) with
 * {@code mysqlbinlog --read-from-remote-server --start-position=...}; the header and
 * the trailing CRC32 are stripped, which is exactly what the deserializer receives.
 *
 * The last case is the same event as a standard MySQL server writes it
 * (flags 0x0001, no vendor bytes) and must keep decoding exactly as before, so the
 * patch cannot regress the plain MySQL path.
 */
public final class GoldenDbTableMapSelfTest {

    private static int failures = 0;

    public static void main(String[] args) {
        check("GoldenDB test.t_test (int, varchar)",
            "88 00 00 00 00 00 01 1f 01 00 00 00 01 00 00 00 04 74 65 73 74 00 06 74 5f 74 65"
                + " 73 74 00 02 03 0f 02 fc 03 02 01 01 00 02 01 2e",
            136L, "test", "t_test", new byte[] {3, 15}, new int[] {0, 1020});

        check("GoldenDB test.t_probe8 (int, varchar, decimal, datetime)",
            "96 00 00 00 00 00 01 1f 01 00 00 00 01 00 00 00 04 74 65 73 74 00 08 74 5f 70 72"
                + " 6f 62 65 38 00 04 03 0f f6 12 05 28 00 0a 02 00 0e 01 01 00 02 01 2e",
            150L, "test", "t_probe8", new byte[] {3, 15, (byte) 0xf6, 18}, new int[] {0, 40, 522, 0});

        check("REGRESSION standard MySQL (flags 0x0001, no vendor bytes)",
            "88 00 00 00 00 00 01 00 04 74 65 73 74 00 06 74 5f 74 65 73 74 00 02 03 0f 02 fc"
                + " 03 02 01 01 00 02 01 2e",
            136L, "test", "t_test", new byte[] {3, 15}, new int[] {0, 1020});

        if (failures > 0) {
            System.out.println(failures + " case(s) FAILED");
            System.exit(1);
        }
        System.out.println("all cases passed");
    }

    private static void check(String label, String hex, long tableId, String database,
                              String table, byte[] types, int[] metadata) {
        try {
            TableMapEventData data = new TableMapEventDataDeserializer()
                .deserialize(new ByteArrayInputStream(parseHex(hex)));
            assertEquals(label, "tableId", tableId, data.getTableId());
            assertEquals(label, "database", database, data.getDatabase());
            assertEquals(label, "table", table, data.getTable());
            assertEquals(label, "columnTypes", Arrays.toString(types),
                Arrays.toString(data.getColumnTypes()));
            assertEquals(label, "columnMetadata", Arrays.toString(metadata),
                Arrays.toString(data.getColumnMetadata()));
            System.out.println("  ok    " + label);
        } catch (Throwable t) {
            failures++;
            System.out.println("  FAIL  " + label + " -> " + t);
        }
    }

    private static void assertEquals(String label, String field, Object expected, Object actual) {
        if (!String.valueOf(expected).equals(String.valueOf(actual))) {
            failures++;
            System.out.println("  FAIL  " + label + ": " + field
                + " expected " + expected + " but was " + actual);
        }
    }

    private static byte[] parseHex(String hex) {
        String[] parts = hex.split(" ");
        byte[] bytes = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            bytes[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return bytes;
    }
}
