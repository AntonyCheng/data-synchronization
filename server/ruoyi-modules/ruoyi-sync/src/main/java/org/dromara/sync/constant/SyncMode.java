package org.dromara.sync.constant;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;

import java.util.Locale;
import java.util.Set;

/** Synchronization modes accepted for tasks and task groups. */
public final class SyncMode {

    /** Bounded snapshot only (BATCH job). */
    public static final String FULL = "FULL";
    /** Binlog only, from a configured startup position (STREAMING job). */
    public static final String INCREMENTAL = "INCREMENTAL";
    /** Snapshot followed by binlog (STREAMING job). Default. */
    public static final String FULL_CDC = "FULL_CDC";

    private static final Set<String> ALL = Set.of(FULL, INCREMENTAL, FULL_CDC);

    private SyncMode() {
    }

    /** Blank resolves to {@link #FULL_CDC}; anything outside the vocabulary is rejected. */
    public static String normalize(String value) {
        String mode = StringUtils.defaultIfBlank(value, FULL_CDC).toUpperCase(Locale.ROOT);
        if (!ALL.contains(mode)) throw new ServiceException("不支持的同步模式：" + mode);
        return mode;
    }

    public static boolean isFull(String value) {
        return FULL.equalsIgnoreCase(value);
    }
}
