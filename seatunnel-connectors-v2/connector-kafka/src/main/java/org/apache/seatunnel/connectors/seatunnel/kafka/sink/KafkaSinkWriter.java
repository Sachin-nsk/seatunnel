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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TRANSACTION_PREFIX;

/** KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka. */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;

    private String transactionPrefix;
    private long lastCheckpointId = 0;
    private SeaTunnelRowType seaTunnelRowType;

    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    /** UPDATE_BEFORE row waiting for its UPDATE_AFTER when merge_update_event is enabled. */
    private SeaTunnelRow pendingUpdateBefore;

    private static final int PREFIX_RANGE = 10000;

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this.context = context;
        this.seaTunnelRowType = seaTunnelRowType;

        if (pluginConfig.get(TRANSACTION_PREFIX) != null) {
            this.transactionPrefix = pluginConfig.get(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }

        restoreState(kafkaStates);
        this.seaTunnelRowSerializer = KafkaSinkSerializer.create(pluginConfig, seaTunnelRowType);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            this.kafkaProducerSender =
                    new KafkaTransactionSender<>(
                            this.transactionPrefix, getKafkaProperties(pluginConfig));
            // abort all transaction number bigger than current transaction, because they maybe
            // already start
            //  transaction.
            if (!kafkaStates.isEmpty()) {
                this.kafkaProducerSender.abortTransaction(kafkaStates.get(0).getCheckpointId() + 1);
            }
            this.kafkaProducerSender.beginTransaction(
                    generateTransactionId(this.transactionPrefix, this.lastCheckpointId + 1));
        } else {
            this.kafkaProducerSender =
                    new KafkaNoTransactionSender<>(getKafkaProperties(pluginConfig));
        }
    }

    /** Creates a writer with a given serializer and sender, for tests only. */
    @VisibleForTesting
    KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer,
            KafkaProduceSender<byte[], byte[]> kafkaProducerSender) {
        this.context = context;
        this.seaTunnelRowSerializer = seaTunnelRowSerializer;
        this.kafkaProducerSender = kafkaProducerSender;
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (seaTunnelRowSerializer.supportsUpdateMerge()) {
            writeWithUpdateMerge(element);
            return;
        }
        ProducerRecord<byte[], byte[]> producerRecord =
                seaTunnelRowSerializer.serializeRow(element);
        kafkaProducerSender.send(producerRecord);
    }

    /**
     * Holds an UPDATE_BEFORE row until the next row arrives. If that row is the matching
     * UPDATE_AFTER and routes to the same topic, partition and key, the pair is sent as one record.
     * Otherwise the held row is sent on its own, as it would be without merging, so no row is
     * dropped or reordered.
     */
    private void writeWithUpdateMerge(SeaTunnelRow element) {
        if (pendingUpdateBefore != null
                && element.getRowKind() == RowKind.UPDATE_AFTER
                && seaTunnelRowSerializer.canMergeUpdate(pendingUpdateBefore, element)) {
            kafkaProducerSender.send(
                    seaTunnelRowSerializer.serializeUpdate(pendingUpdateBefore, element));
            pendingUpdateBefore = null;
            return;
        }
        flushPendingUpdateBefore();
        if (element.getRowKind() == RowKind.UPDATE_BEFORE) {
            // Copy the row because upstream may reuse the instance for the next row.
            pendingUpdateBefore = element.copy();
        } else {
            kafkaProducerSender.send(seaTunnelRowSerializer.serializeRow(element));
        }
    }

    /** Sends a held UPDATE_BEFORE row on its own, as a DELETE message. */
    private void flushPendingUpdateBefore() {
        if (pendingUpdateBefore != null) {
            SeaTunnelRow updateBefore = pendingUpdateBefore;
            pendingUpdateBefore = null;
            kafkaProducerSender.send(seaTunnelRowSerializer.serializeRow(updateBefore));
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
        this.lastCheckpointId = checkpointId;
        this.kafkaProducerSender.beginTransaction(
                generateTransactionId(this.transactionPrefix, this.lastCheckpointId + 1));
        return states;
    }

    /**
     * Prepares the commit for the current checkpoint. A held UPDATE_BEFORE row is sent first, so it
     * belongs to this checkpoint (and this transaction in exactly-once mode) and no pending row
     * needs to be kept in the writer state.
     */
    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        flushPendingUpdateBefore();
        return kafkaProducerSender.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        kafkaProducerSender.abortTransaction();
    }

    @Override
    public void close() {
        try {
            try {
                flushPendingUpdateBefore();
            } finally {
                kafkaProducerSender.close();
            }
        } catch (Exception e) {
            throw new KafkaConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Close kafka sink writer error",
                    e);
        }
    }

    private Properties getKafkaProperties(ReadonlyConfig pluginConfig) {
        Properties kafkaProperties = new Properties();
        if (pluginConfig.get(KAFKA_CONFIG) != null) {
            pluginConfig.get(KAFKA_CONFIG).forEach((key, value) -> kafkaProperties.put(key, value));
        }

        if (pluginConfig.get(ASSIGN_PARTITIONS) != null) {
            kafkaProperties.put(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
            kafkaProperties.put(
                    MessageContentPartitioner.ASSIGN_PARTITIONS_CONFIG,
                    pluginConfig.get(ASSIGN_PARTITIONS));
        }

        kafkaProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.get(BOOTSTRAP_SERVERS));
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
    }

    private KafkaSemantics getKafkaSemantics(ReadonlyConfig pluginConfig) {
        if (pluginConfig.get(SEMANTICS) != null) {
            return pluginConfig.get(SEMANTICS);
        }
        return KafkaSemantics.NON;
    }

    protected static String generateTransactionId(String transactionPrefix, long checkpointId) {
        return transactionPrefix + "-" + checkpointId;
    }

    private void restoreState(List<KafkaSinkState> states) {
        if (!states.isEmpty()) {
            this.transactionPrefix = states.get(0).getTransactionIdPrefix();
            this.lastCheckpointId = states.get(0).getCheckpointId();
        }
    }
}
