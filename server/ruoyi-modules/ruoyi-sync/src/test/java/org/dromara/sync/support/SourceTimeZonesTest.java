package org.dromara.sync.support;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.vo.DataSourceCheckItemVo;
import org.dromara.sync.domain.vo.DataSourceTimeZoneVo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class SourceTimeZonesTest {

    private static final Instant WINTER = Instant.parse("2026-01-15T00:00:00Z");
    private static final Instant SUMMER = Instant.parse("2026-07-15T00:00:00Z");

    @Test
    void blankMeansTheCompatibleDefault() {
        assertEquals("Asia/Shanghai", SourceTimeZones.effective(null));
        for (String blank : new String[]{null, "", "   "}) {
            assertEquals("Asia/Shanghai", SourceTimeZones.effective(source(blank)), "'" + blank + "'");
        }
        assertEquals("UTC", SourceTimeZones.effective(source(" UTC ")));
        assertEquals("Europe/Berlin", SourceTimeZones.effective(source("europe/berlin")), "the engine always gets the canonical id");
        assertEquals("CST", SourceTimeZones.effective(source(" CST ")), "an invalid stored value is returned for naming, not guessed");
    }

    /** Only a value written around the service can be invalid; the precheck must report it, not fail. */
    @Test
    void anInvalidStoredZoneIsReportedByTheVerdict() {
        DataSourceTimeZoneVo verdict = SourceTimeZones.verdict(source("CST"), SourceTimeZones.detect("SYSTEM", "CST", 8 * 3600, WINTER), WINTER);
        assertEquals(Boolean.FALSE, verdict.getMatched());
        assertNull(verdict.getEffectiveUtcOffset());
        assertTrue(verdict.getMessage().contains("不是有效的 IANA 时区 ID"), verdict.getMessage());
        assertEquals("Asia/Shanghai", verdict.getSuggestedTimeZone());
        assertFalse(SourceTimeZones.checkItem(verdict).getPassed());
    }

    @Test
    void normalizeKeepsOmittedAndBlankAndCanonicalisesIanaIds() {
        assertNull(SourceTimeZones.normalize(null), "omitted stays omitted: an edit keeps the stored zone");
        assertEquals("", SourceTimeZones.normalize("  "), "blank clears the zone back to compatibility mode");
        assertEquals("UTC", SourceTimeZones.normalize("utc"));
        assertEquals("Asia/Shanghai", SourceTimeZones.normalize(" asia/shanghai "));
        assertEquals("America/New_York", SourceTimeZones.normalize("America/New_York"));
        assertEquals("Etc/GMT-8", SourceTimeZones.normalize("Etc/GMT-8"));
    }

    /** CST is China, US Central or Cuba; IST is India, Ireland or Israel. The engine must never guess. */
    @Test
    void normalizeRejectsAbbreviationsOffsetsAndUnknownIds() {
        for (String abbreviation : new String[]{"CST", "cst", "IST", "EST", "PST", "JST"}) {
            ServiceException ex = assertThrows(ServiceException.class, () -> SourceTimeZones.normalize(abbreviation), abbreviation);
            assertTrue(ex.getMessage().contains("缩写"), ex.getMessage());
            assertFalse(SourceTimeZones.isValid(abbreviation), abbreviation);
        }
        for (String invalid : new String[]{"+08:00", "GMT+8", "UTC+08:00", "Mars/Olympus", "Asia Shanghai"}) {
            ServiceException ex = assertThrows(ServiceException.class, () -> SourceTimeZones.normalize(invalid), invalid);
            assertTrue(ex.getMessage().contains("无效"), ex.getMessage());
        }
    }

    @Test
    void urlEncodingKeepsTheCompatibleDefaultByteIdentical() {
        assertEquals("Asia%2FShanghai", SourceTimeZones.urlEncoded("Asia/Shanghai"));
        assertEquals("UTC", SourceTimeZones.urlEncoded("UTC"));
        assertEquals("Etc%2FGMT%2B5", SourceTimeZones.urlEncoded("Etc/GMT+5"), "a raw '+' would be decoded as a space");
    }

    @Test
    void detectionResolvesSystemAndSuggestsAnIanaId() {
        // The local stack: --default-time-zone=+00:00, TZ=UTC.
        SourceTimeZones.Detection stack = SourceTimeZones.detect("+00:00", "UTC", 0, WINTER);
        assertEquals("+00:00", stack.display());
        assertEquals(0, stack.offsetSeconds());
        assertEquals("UTC", stack.suggestion());

        SourceTimeZones.Detection system = SourceTimeZones.detect("SYSTEM", "UTC", null, WINTER);
        assertEquals("SYSTEM（UTC）", system.display());
        assertEquals(0, system.offsetSeconds());
        assertEquals("UTC", system.suggestion());

        // GoldenDB / a Chinese server: the abbreviation is never interpreted, the measured offset decides.
        SourceTimeZones.Detection china = SourceTimeZones.detect("SYSTEM", "CST", 8 * 3600, WINTER);
        assertEquals("SYSTEM（CST）", china.display());
        assertEquals("Asia/Shanghai", china.suggestion(), "+08:00 suggests the compatible default - filling it in changes nothing");
        assertEquals("Asia/Shanghai", SourceTimeZones.detect("+8:00", null, null, WINTER).suggestion());

        SourceTimeZones.Detection chicago = SourceTimeZones.detect("SYSTEM", "CST", -6 * 3600, WINTER);
        assertEquals("Etc/GMT+6", chicago.suggestion(), "whole-hour offsets without a usable name get the fixed Etc zone");
        assertNull(SourceTimeZones.detect("+05:30", null, null, WINTER).suggestion(), "no fixed IANA zone for half hours");
        assertEquals(19800, SourceTimeZones.detect("+05:30", null, null, WINTER).offsetSeconds());

        SourceTimeZones.Detection berlin = SourceTimeZones.detect("Europe/Berlin", "UTC", null, SUMMER);
        assertEquals("Europe/Berlin", berlin.suggestion(), "a named server zone is kept, DST and all");
        assertEquals(7200, berlin.offsetSeconds());

        SourceTimeZones.Detection unknown = SourceTimeZones.detect(null, null, null, WINTER);
        assertNull(unknown.display());
        assertNull(unknown.offsetSeconds());
        assertNull(unknown.suggestion());
    }

    /** The bug this feature exists for: compatibility mode (Asia/Shanghai) against the UTC local stack. */
    @Test
    void aCompatibilityModeSourceOnAUtcServerIsFlaggedWithTheShiftAndTheFix() {
        DataSourceTimeZoneVo verdict = SourceTimeZones.verdict(source(null), SourceTimeZones.detect("+00:00", "UTC", 0, WINTER), WINTER);
        assertEquals(Boolean.FALSE, verdict.getMatched());
        assertEquals("8", verdict.getShiftHours());
        assertEquals("", verdict.getConfiguredTimeZone());
        assertEquals("Asia/Shanghai", verdict.getEffectiveTimeZone());
        assertEquals("UTC+08:00", verdict.getEffectiveUtcOffset());
        assertEquals("UTC+00:00", verdict.getServerUtcOffset());
        assertEquals("UTC", verdict.getSuggestedTimeZone());
        assertTrue(verdict.getMessage().startsWith("增量阶段 TIMESTAMP 将偏移 +8 小时（比源库显示值晚 8 小时）"), verdict.getMessage());
        assertTrue(verdict.getMessage().contains("兼容模式"), verdict.getMessage());

        DataSourceCheckItemVo item = SourceTimeZones.checkItem(verdict);
        assertEquals("timezone", item.getCode());
        assertFalse(item.getRequired(), "a zone mismatch is a warning, not a blocker");
        assertFalse(item.getPassed());
        assertEquals("+00:00，UTC+00:00", item.getActual());
        assertTrue(item.getSuggestion().contains("UTC") && item.getSuggestion().contains("重新初始化"), item.getSuggestion());
    }

    @Test
    void offsetsAreComparedNotNames() {
        // An explicit UTC source on the UTC stack.
        DataSourceTimeZoneVo utc = SourceTimeZones.verdict(source("UTC"), SourceTimeZones.detect("+00:00", "UTC", 0, WINTER), WINTER);
        assertEquals(Boolean.TRUE, utc.getMatched());
        assertEquals("0", utc.getShiftHours());
        assertTrue(SourceTimeZones.checkItem(utc).getPassed());

        // GoldenDB's CST at +8 matches compatibility mode: today's behaviour is already right there.
        DataSourceTimeZoneVo goldenDb = SourceTimeZones.verdict(source(""), SourceTimeZones.detect("SYSTEM", "CST", 8 * 3600, WINTER), WINTER);
        assertEquals(Boolean.TRUE, goldenDb.getMatched());

        // New York is -5 in January and -4 in July; the verdict follows the instant it is taken at.
        DataSourceTimeZoneVo newYork = SourceTimeZones.verdict(source("America/New_York"), SourceTimeZones.detect("SYSTEM", "EDT", -4 * 3600, SUMMER), SUMMER);
        assertEquals(Boolean.TRUE, newYork.getMatched());
        DataSourceTimeZoneVo early = SourceTimeZones.verdict(source(null), SourceTimeZones.detect("SYSTEM", "EDT", -4 * 3600, SUMMER), SUMMER);
        assertEquals("12", early.getShiftHours());
        DataSourceTimeZoneVo late = SourceTimeZones.verdict(source("UTC"), SourceTimeZones.detect("+05:30", null, null, WINTER), WINTER);
        assertEquals("-5.5", late.getShiftHours());
        assertTrue(late.getMessage().contains("比源库显示值早 5.5 小时"), late.getMessage());
    }

    @Test
    void anUnreadableServerZoneIsAnUnconfirmedWarning() {
        DataSourceTimeZoneVo verdict = SourceTimeZones.verdict(source("UTC"), SourceTimeZones.detect(null, null, null, WINTER), WINTER);
        assertNull(verdict.getMatched());
        assertNull(verdict.getShiftHours());
        DataSourceCheckItemVo item = SourceTimeZones.checkItem(verdict);
        assertFalse(item.getPassed());
        assertEquals("未知", item.getActual());
        assertTrue(item.getMessage().contains("无法读取源库时区"), item.getMessage());
    }

    private static DataSource source(String zone) {
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setServerTimeZone(zone);
        return source;
    }
}
