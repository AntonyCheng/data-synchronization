package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.domain.SyncTaskConfigVersion;
import org.dromara.sync.domain.vo.SyncTaskConfigVersionVo;

public interface SyncTaskConfigVersionMapper extends BaseMapperPlus<SyncTaskConfigVersion, SyncTaskConfigVersionVo> {

    default int deleteByTaskId(Long taskId) {
        return delete(new LambdaQueryWrapper<SyncTaskConfigVersion>().eq(SyncTaskConfigVersion::getTaskId, taskId));
    }
}
