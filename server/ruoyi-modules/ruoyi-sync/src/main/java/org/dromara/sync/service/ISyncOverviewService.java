package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SyncOverviewVo;

/** Read-only dashboard aggregates over tasks, task groups, their table items and data sources. */
public interface ISyncOverviewService {

    SyncOverviewVo overview();
}
