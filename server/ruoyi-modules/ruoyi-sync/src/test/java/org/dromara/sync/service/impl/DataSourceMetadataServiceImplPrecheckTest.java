package org.dromara.sync.service.impl;

import org.dromara.sync.domain.vo.DataSourceCheckItemVo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("dev")
class DataSourceMetadataServiceImplPrecheckTest {

    private static final DataSourceCheckItemVo BINLOG_OK = new DataSourceCheckItemVo("log_bin", "binlog 已开启", true, true, "ON", "配置符合要求");
    private static final DataSourceCheckItemVo BINLOG_OFF = new DataSourceCheckItemVo("log_bin", "binlog 已开启", true, false, "OFF", "必须开启 log_bin");
    private static final DataSourceCheckItemVo ZONE_OK = new DataSourceCheckItemVo("timezone", "源端时区", false, true, "+00:00，UTC+00:00", "源库时区与数据源时区一致");
    private static final DataSourceCheckItemVo ZONE_SHIFT = new DataSourceCheckItemVo("timezone", "源端时区", false, false, "+00:00，UTC+00:00",
        "增量阶段 TIMESTAMP 将偏移 +8 小时（比源库显示值晚 8 小时）");

    /** The wizards show only the summary line, so a failed optional item must be named in it. */
    @Test
    void aZoneMismatchIsNamedInTheSummaryWithoutFailingThePrecheck() {
        assertEquals("MySQL binlog CDC 前置检查通过", DataSourceMetadataServiceImpl.precheckMessage(List.of(BINLOG_OK, ZONE_OK)));
        assertEquals("MySQL binlog CDC 前置检查通过。注意：增量阶段 TIMESTAMP 将偏移 +8 小时（比源库显示值晚 8 小时）",
            DataSourceMetadataServiceImpl.precheckMessage(List.of(BINLOG_OK, ZONE_SHIFT)));
        assertEquals("存在必须修复的 MySQL binlog CDC 前置条件。注意：增量阶段 TIMESTAMP 将偏移 +8 小时（比源库显示值晚 8 小时）",
            DataSourceMetadataServiceImpl.precheckMessage(List.of(BINLOG_OFF, ZONE_SHIFT)));
    }
}
