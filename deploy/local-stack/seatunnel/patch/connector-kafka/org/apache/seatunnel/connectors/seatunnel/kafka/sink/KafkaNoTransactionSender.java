/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SeaTunnel 2.3.13's sender for the {@code AT_LEAST_ONCE} / {@code NON} semantics with one change: a
 * record the producer fails to deliver fails the writer instead of vanishing.
 *
 * <p>The stock class calls {@code KafkaProducer.send(record)} and drops the returned future, and its
 * {@code snapshotState} only calls {@code flush()}, which does not report failed sends. A record the
 * producer refuses - larger than {@code max.request.size}, larger than the topic's
 * {@code max.message.bytes}, a topic it may not write - was therefore lost without a trace: the job
 * kept running, the checkpoint completed and moved the source offset past the row.
 *
 * <p>Here every send carries a callback that keeps the first failure, and the next {@code send},
 * checkpoint ({@code snapshotState}, after the flush) or {@code close} throws it. The checkpoint that
 * would have covered the record never completes, so the job restarts from the previous one and - when
 * the failure persists - ends FAILED with the cause as its error.
 */
public class KafkaNoTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;
    /** The first delivery failure; reported by the next send, checkpoint or close. */
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public KafkaNoTransactionSender(Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        throwIfFailed();
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) failure.compareAndSet(null, exception);
        });
    }

    @Override
    public void beginTransaction(String transactionId) {
        // no-op
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortTransaction() {
        // no-op
    }

    @Override
    public void abortTransaction(long checkpointId) {
        // no-op
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        kafkaProducer.flush();
        throwIfFailed();
        return Collections.emptyList();
    }

    @Override
    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
        throwIfFailed();
    }

    private void throwIfFailed() {
        Exception cause = failure.get();
        if (cause != null) {
            throw new KafkaException("Kafka sink could not deliver a record ("
                + cause.getClass().getSimpleName() + ": " + cause.getMessage() + ")", cause);
        }
    }
}
