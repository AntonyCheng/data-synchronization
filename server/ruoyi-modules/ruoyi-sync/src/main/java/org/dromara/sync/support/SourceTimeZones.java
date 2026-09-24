package org.dromara.sync.support;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.vo.DataSourceCheckItemVo;
import org.dromara.sync.domain.vo.DataSourceTimeZoneVo;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The time zone of a MySQL source server, as the engine has to be told it.
 *
 * <p>Binlog events carry a {@code TIMESTAMP} as a UTC instant. The MySQL-CDC source turns it
 * back into the zone-less wall clock that SeaTunnel carries, using {@code server-time-zone}.
 * The snapshot phase reads the server's own rendering instead. The two phases therefore agree
 * only when {@code server-time-zone} is the zone the server really renders in. When it is not,
 * every binlog-phase {@code TIMESTAMP} is shifted by the difference, whatever the target.
 * {@code DATETIME}, {@code DATE} and {@code TIME} carry no zone and are not affected.
 *
 * <p>The zone is configured per data source ({@code ds_data_source.server_time_zone}). Blank
 * means {@link #COMPATIBLE_DEFAULT}, the value that was hard-coded before the column existed,
 * so the generated configs - and the fingerprints of existing CDC tasks - stay unchanged
 * until an operator sets it.
 */
public final class SourceTimeZones {

    /** The zone of a MySQL source with no configured zone; the only value before the column existed. */
    public static final String COMPATIBLE_DEFAULT = "Asia/Shanghai";

    private static final Pattern OFFSET = Pattern.compile("[+-]\\d{1,2}:\\d{2}");

    /** IANA ids by upper-cased spelling, so {@code utc} or {@code asia/shanghai} still resolve. */
    private static final Map<String, String> IANA_IDS = new TreeMap<>();

    static {
        for (String id : ZoneId.getAvailableZoneIds()) IANA_IDS.put(id.toUpperCase(Locale.ROOT), id);
    }

    private SourceTimeZones() {
    }

    /**
     * The zone the engine is told for {@code source}: the configured one in its canonical
     * spelling, else {@link #COMPATIBLE_DEFAULT}. A stored value that is not a valid id (only
     * possible when written around the service) comes back trimmed but otherwise as is, so
     * callers can name it; check it with {@link #isValid}.
     */
    public static String effective(DataSource source) {
        String configured = source == null ? null : source.getServerTimeZone();
        if (StringUtils.isBlank(configured)) return COMPATIBLE_DEFAULT;
        String trimmed = configured.trim();
        return isValid(trimmed) ? IANA_IDS.get(trimmed.toUpperCase(Locale.ROOT)) : trimmed;
    }

    /** The zone as a JDBC URL parameter value ({@code Asia/Shanghai} becomes {@code Asia%2FShanghai}). */
    public static String urlEncoded(String zoneId) {
        return URLEncoder.encode(zoneId, StandardCharsets.UTF_8);
    }

    /**
     * The value to store for an operator's input: {@code null} stays {@code null} (not sent),
     * blank becomes {@code ""} (compatibility mode), anything else must be an IANA region id and
     * comes back in its canonical spelling.
     *
     * @throws ServiceException for an abbreviation such as {@code CST} or {@code IST}, which
     *                          names several zones, a bare offset, or an unknown id
     */
    public static String normalize(String value) {
        if (value == null) return null;
        String input = value.trim();
        if (input.isEmpty()) return "";
        String upper = input.toUpperCase(Locale.ROOT);
        if (ZoneId.SHORT_IDS.containsKey(upper)) {
            throw new ServiceException("服务器时区“" + input + "”是时区缩写，含义不唯一（例如 CST 可以是中国、美国中部或古巴时间），"
                + "请填写 IANA 时区 ID，例如 Asia/Shanghai、UTC、America/Chicago");
        }
        String canonical = IANA_IDS.get(upper);
        if (canonical == null) {
            throw new ServiceException("无效的服务器时区“" + input + "”，请填写 IANA 时区 ID，例如 UTC、Asia/Shanghai、Europe/Berlin"
                + "（固定偏移可用 Etc/GMT-8 表示 UTC+8）");
        }
        return canonical;
    }

    /** True when {@code id} would be accepted by {@link #normalize}. */
    public static boolean isValid(String id) {
        if (StringUtils.isBlank(id)) return false;
        String upper = id.trim().toUpperCase(Locale.ROOT);
        return !ZoneId.SHORT_IDS.containsKey(upper) && IANA_IDS.containsKey(upper);
    }

    // ------------------------------------------------------------------ detection

    /**
     * Reads what the server renders {@code TIMESTAMP} in on a platform-side connection. The
     * offset is measured ({@code NOW()} against {@code UTC_TIMESTAMP()} in the session), so an
     * abbreviation such as {@code CST} never has to be interpreted. The session zone is the
     * server default: the platform URL's {@code serverTimezone} is not forced onto the session
     * ({@code forceConnectionTimeZoneToSession} defaults to false; verified with Connector/J
     * 8.0.33 and 9.7.0). A failed read returns an all-unknown detection rather than failing the
     * caller.
     */
    public static Detection read(Connection connection, Instant now) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                 "select @@time_zone, @@system_time_zone, timestampdiff(second, utc_timestamp(), now())")) {
            if (!resultSet.next()) return detect(null, null, null, now);
            long offset = resultSet.getLong(3);
            Integer measured = resultSet.wasNull() ? null : Math.toIntExact(offset);
            return detect(resultSet.getString(1), resultSet.getString(2), measured, now);
        } catch (SQLException | ArithmeticException ex) {
            return detect(null, null, null, now);
        }
    }

    /**
     * What the server's zone variables say. {@code SYSTEM} is resolved through
     * {@code system_time_zone}. The offset is the measured one when available, else the one
     * implied by the name. The suggestion is the id an operator should configure:
     * <ul>
     *   <li>the server's own name when it is a valid IANA id (e.g. {@code UTC}, {@code Europe/Berlin});</li>
     *   <li>otherwise (a bare offset such as {@code +08:00}, or an abbreviation such as {@code CST})
     *       inferred from the offset: the offset of {@link #COMPATIBLE_DEFAULT} suggests it (filling
     *       it in generates the same config as leaving the field blank), 0 suggests {@code UTC}, any
     *       other whole hour suggests the fixed {@code Etc/GMT∓N} zone, which knows no daylight
     *       saving - an operator in a region that observes it should pick the region instead.</li>
     * </ul>
     */
    public static Detection detect(String timeZone, String systemTimeZone, Integer measuredOffsetSeconds, Instant now) {
        String zone = StringUtils.trimToEmpty(timeZone);
        String system = StringUtils.trimToEmpty(systemTimeZone);
        boolean usesSystem = "SYSTEM".equalsIgnoreCase(zone);
        String name = usesSystem ? system : zone;
        String display = zone.isEmpty() ? null : usesSystem ? "SYSTEM（" + (system.isEmpty() ? "未知" : system) + "）" : zone;
        Integer offset = measuredOffsetSeconds != null ? measuredOffsetSeconds : impliedOffset(name, now);
        String suggestion = isValid(name) ? normalize(name) : offset == null ? null : suggestionFor(offset, now);
        return new Detection(display, offset, suggestion);
    }

    private static Integer impliedOffset(String name, Instant now) {
        if (StringUtils.isBlank(name)) return null;
        if (OFFSET.matcher(name).matches()) {
            try {
                return ZoneOffset.of(name.length() == 5 ? name.charAt(0) + "0" + name.substring(1) : name).getTotalSeconds();
            } catch (RuntimeException ex) {
                return null;
            }
        }
        return isValid(name) ? offsetSeconds(normalize(name), now) : null;
    }

    private static String suggestionFor(int offsetSeconds, Instant now) {
        if (offsetSeconds == offsetSeconds(COMPATIBLE_DEFAULT, now)) return COMPATIBLE_DEFAULT;
        if (offsetSeconds == 0) return "UTC";
        if (offsetSeconds % 3600 != 0) return null;
        int hours = offsetSeconds / 3600;
        // POSIX sign convention: Etc/GMT-8 is UTC+8.
        String id = "Etc/GMT" + (hours > 0 ? "-" : "+") + Math.abs(hours);
        return isValid(id) ? id : null;
    }

    /** The current UTC offset of an IANA zone, in seconds. */
    public static int offsetSeconds(String zoneId, Instant now) {
        return ZoneId.of(zoneId).getRules().getOffset(now).getTotalSeconds();
    }

    // ------------------------------------------------------------------ verdict

    /**
     * Compares the zone the engine will be told for {@code source} with what the server
     * renders in, at {@code now}. Offsets are compared rather than names, because names like
     * {@code CST} are ambiguous and {@code +08:00} and {@code Asia/Shanghai} are the same today.
     */
    public static DataSourceTimeZoneVo verdict(DataSource source, Detection detection, Instant now) {
        String effective = effective(source);
        boolean configured = source != null && StringUtils.isNotBlank(source.getServerTimeZone());
        DataSourceTimeZoneVo result = new DataSourceTimeZoneVo();
        result.setServerTimeZone(detection.display());
        result.setServerUtcOffset(detection.offsetSeconds() == null ? null : formatOffset(detection.offsetSeconds()));
        result.setSuggestedTimeZone(detection.suggestion());
        result.setConfiguredTimeZone(configured ? effective : "");
        result.setEffectiveTimeZone(effective);
        if (!isValid(effective)) {
            result.setMatched(false);
            result.setMessage("数据源的服务器时区“" + effective + "”不是有效的 IANA 时区 ID，任务无法生成引擎配置，请修改数据源");
            return result;
        }
        int effectiveOffset = offsetSeconds(effective, now);
        result.setEffectiveUtcOffset(formatOffset(effectiveOffset));
        String effectiveLabel = "数据源时区 " + effective + (configured ? "" : "（兼容模式）") + "，当前 " + formatOffset(effectiveOffset);
        if (detection.offsetSeconds() == null) {
            result.setMatched(null);
            result.setMessage("无法读取源库时区，不能确认增量阶段 TIMESTAMP 是否偏移；" + effectiveLabel);
            return result;
        }
        int shift = effectiveOffset - detection.offsetSeconds();
        result.setShiftHours(hours(shift));
        result.setMatched(shift == 0);
        String serverLabel = "源库 " + (detection.display() == null ? "未知" : detection.display())
            + "，当前 " + formatOffset(detection.offsetSeconds());
        result.setMessage(shift == 0
            ? "源库时区与数据源时区一致：" + effectiveLabel + "；" + serverLabel
            : "增量阶段 TIMESTAMP 将偏移 " + signedHours(shift) + " 小时（比源库显示值" + (shift > 0 ? "晚 " : "早 ")
                + hours(Math.abs(shift)) + " 小时）：" + effectiveLabel + "；" + serverLabel);
        return result;
    }

    /** The optional ({@code required = false}) CDC precheck item for {@code verdict}. */
    public static DataSourceCheckItemVo checkItem(DataSourceTimeZoneVo verdict) {
        boolean passed = Boolean.TRUE.equals(verdict.getMatched());
        String actual = verdict.getServerTimeZone() == null ? "未知"
            : verdict.getServerTimeZone() + (verdict.getServerUtcOffset() == null ? "" : "，" + verdict.getServerUtcOffset());
        DataSourceCheckItemVo item = new DataSourceCheckItemVo("timezone", "源端时区", false, passed, actual, verdict.getMessage());
        if (!passed) {
            String target = StringUtils.isNotBlank(verdict.getSuggestedTimeZone())
                ? verdict.getSuggestedTimeZone() : "与源库一致的 IANA 时区";
            item.setSuggestion("把数据源的“服务器时区”设为 " + target + "；已有的 CDC 任务需重新初始化后才会使用新时区");
        } else {
            item.setSuggestion("无需修改");
        }
        return item;
    }

    /** {@code UTC+08:00} / {@code UTC-05:00} / {@code UTC+00:00}. */
    public static String formatOffset(int offsetSeconds) {
        String id = ZoneOffset.ofTotalSeconds(offsetSeconds).getId();
        return "UTC" + ("Z".equals(id) ? "+00:00" : id);
    }

    private static String hours(int seconds) {
        return BigDecimal.valueOf(seconds).divide(BigDecimal.valueOf(3600), 2, RoundingMode.HALF_UP)
            .stripTrailingZeros().toPlainString();
    }

    private static String signedHours(int seconds) {
        return (seconds > 0 ? "+" : "") + hours(seconds);
    }

    /** What the server reported: a display name, its current UTC offset in seconds, and the id to configure. */
    public record Detection(String display, Integer offsetSeconds, String suggestion) {
    }
}
