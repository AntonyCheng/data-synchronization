package org.dromara.sync.support;

import org.dromara.common.core.utils.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Small text helpers shared by the sync services: fingerprints, column-safe truncation, secret redaction. */
public final class SyncText {

    /**
     * Byte budget for {@code last_error} / {@code last_check_message} style columns. The
     * columns are varchar(2000); 1800 UTF-8 bytes leaves headroom for the surrounding
     * message text that some callers prepend.
     */
    private static final int COLUMN_BYTE_LIMIT = 1800;

    private static final String SECRET_PATTERN = "(?i)(password|pwd)(\\s*[=:]\\s*)[^&,;\\s}]+";

    private SyncText() {
    }

    /** Lower-case hex SHA-256; used for engine config fingerprints and schema snapshot hashes. */
    public static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) result.append(String.format("%02x", item));
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 不可用", ex);
        }
    }

    /**
     * Keep multibyte engine/database error text within the metadata column. Walks back off
     * any UTF-8 continuation byte at the cut point so a truncated Chinese message never
     * ends in a garbled replacement character.
     */
    public static String truncateForColumn(String value) {
        if (value == null) return null;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= COLUMN_BYTE_LIMIT) return value;
        int end = COLUMN_BYTE_LIMIT;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) end--;
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }

    /** Masks {@code password=...} / {@code pwd: ...} fragments that JDBC and Kafka clients echo back in error text. */
    public static String redactSecrets(String text) {
        return text == null ? null : text.replaceAll(SECRET_PATTERN, "$1$2******");
    }

    /** The exception message with secrets masked, or {@code fallback} when the message is blank. */
    public static String safeMessage(Throwable ex, String fallback) {
        String message = ex == null ? null : ex.getMessage();
        return StringUtils.isBlank(message) ? fallback : redactSecrets(message);
    }
}
