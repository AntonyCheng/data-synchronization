package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.support.TableNames;

import java.util.List;

/**
 * The live column list of a source table, which the config generator needs to decide
 * whether a projection transform is required. The answer changes the shape of the config
 * and with it the fingerprint, so a lookup that cannot be answered must throw rather than
 * guess: a guess taken on a flaky connection would make a resume look like a config change.
 */
@FunctionalInterface
public interface SourceColumns {

    /** Column names of {@code tableReference} ({@code table} or {@code db.table}) in table order. */
    List<String> of(String tableReference);

    /** Reads through the platform's metadata introspection, the single path the wizard and prechecks use. */
    static SourceColumns fromMetadata(IDataSourceMetadataService metadataService, DataSource source) {
        return tableReference -> {
            DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(),
                source.getDatabaseName(), TableNames.unqualified(tableReference));
            if (metadata == null || metadata.getColumns() == null || metadata.getColumns().isEmpty()) {
                throw new ServiceException("源表没有可同步字段：" + tableReference);
            }
            return metadata.getColumns().stream().map(DataSourceColumnVo::getName).toList();
        };
    }

    /** A fixed answer, for callers that already hold the schema (or tests). */
    static SourceColumns fixed(List<String> columns) {
        return tableReference -> columns;
    }
}
