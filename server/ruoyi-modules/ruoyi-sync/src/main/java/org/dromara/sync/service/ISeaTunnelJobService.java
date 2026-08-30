package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SeaTunnelJobConfigPreview;
import org.dromara.sync.domain.vo.SeaTunnelJobOperationResult;
import org.dromara.sync.domain.vo.SeaTunnelJobStatus;

/** Platform-facing SeaTunnel adapter contract. */
public interface ISeaTunnelJobService {

    /** Generate a redacted, reviewable job configuration for a task. */
    SeaTunnelJobConfigPreview previewConfig(Long taskId);

    SeaTunnelJobOperationResult start(Long taskId);

    SeaTunnelJobStatus refreshStatus(Long taskId);

    /** Refresh a task after an application restart without failing startup. */
    void recoverRunningTasks();

    SeaTunnelJobOperationResult pause(Long taskId);

    SeaTunnelJobOperationResult resume(Long taskId);

    SeaTunnelJobOperationResult stop(Long taskId);

    /** Drop the old checkpoint/savepoint and execute a fresh full initialization. */
    SeaTunnelJobOperationResult reinitialize(Long taskId);
}
