package org.dromara.sync.service.impl;

import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.dromara.common.core.exception.ServiceException;

@Tag("dev")
class KafkaEventProducerMetricsTest {

    @Test
    void persistsMetricsOnlyAfterTaskExists() {
        SyncTaskMapper mapper = mock(SyncTaskMapper.class);
        when(mapper.selectById(9L)).thenReturn(new SyncTask());
        KafkaEventProducer producer = new KafkaEventProducer(JsonMapper.builder().build(), mapper);
        KafkaEventNormalizer.NormalizedEvent event = new KafkaEventNormalizer.NormalizedEvent(
            "INSERT", JsonMapper.builder().build().createObjectNode().put("id", 1), null, null,
            "CDC", "source_db", "customers", "2026-08-31T00:00:00Z");
        producer.persistMetrics(9L, List.of(event), List.of(new KafkaEventProducer.PublishResult("topic", 2, 17L, "{\"id\":1}")));
        verify(mapper).update(isNull(), any());
    }

    @Test
    void rejectsUnknownTaskBeforePublishing() {
        SyncTaskMapper mapper = mock(SyncTaskMapper.class);
        when(mapper.selectById(99L)).thenReturn(null);
        KafkaEventProducer producer = new KafkaEventProducer(JsonMapper.builder().build(), mapper);
        assertThrows(ServiceException.class, () -> producer.publishForTask(99L, "localhost:9092", "topic", List.of()));
    }
}
