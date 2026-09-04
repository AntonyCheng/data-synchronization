package org.dromara.sync.service.impl;

import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.lang.reflect.Method;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class DataSourceMetadataServiceImplCompatibilityTest {

    private final DataSourceMetadataServiceImpl service = new DataSourceMetadataServiceImpl(null, null);

    @Test
    void mysqlJsonToMysqlJsonIsCompatible() throws Exception {
        assertTrue(compatible(jsonColumn(), jsonColumn(), "MYSQL"));
    }

    @Test
    void mysqlJsonToPostgresqlTextIsCompatible() throws Exception {
        DataSourceColumnVo target = column("text", Types.LONGVARCHAR);
        assertTrue(compatible(jsonColumn(), target, "POSTGRESQL"));
    }

    @Test
    void mysqlJsonToPostgresqlJsonRequiresExplicitCast() throws Exception {
        assertFalse(compatible(jsonColumn(), jsonColumn(), "POSTGRESQL"));
    }

    private boolean compatible(DataSourceColumnVo source, DataSourceColumnVo target, String targetType) throws Exception {
        Method method = DataSourceMetadataServiceImpl.class.getDeclaredMethod(
            "compatible", DataSourceColumnVo.class, DataSourceColumnVo.class, String.class);
        method.setAccessible(true);
        return (boolean) method.invoke(service, source, target, targetType);
    }

    private static DataSourceColumnVo jsonColumn() {
        return column("json", Types.LONGVARCHAR);
    }

    private static DataSourceColumnVo column(String typeName, int jdbcType) {
        DataSourceColumnVo column = new DataSourceColumnVo();
        column.setName("profile");
        column.setTypeName(typeName);
        column.setJdbcType(jdbcType);
        return column;
    }
}
