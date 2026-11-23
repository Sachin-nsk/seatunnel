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

package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface SeaTunnelRowSerializer<K, V> {

    /**
     * Serialize the {@link SeaTunnelRow} to a Kafka {@link ProducerRecord}.
     *
     * @param row seatunnel row
     * @return kafka record.
     */
    ProducerRecord<K, V> serializeRow(SeaTunnelRow row);

    /**
     * Whether this serializer can write an update pair as one record through {@link
     * #serializeUpdate(SeaTunnelRow, SeaTunnelRow)}.
     *
     * @return true if update pairs can be merged
     */
    default boolean supportsUpdateMerge() {
        return false;
    }

    /**
     * Whether an update pair can be written as one record without changing where it is routed. The
     * rows must belong to the same table and resolve to the same topic, partition and message key.
     *
     * @param before the {@code UPDATE_BEFORE} row
     * @param after the {@code UPDATE_AFTER} row
     * @return true if the pair can be merged
     */
    default boolean canMergeUpdate(SeaTunnelRow before, SeaTunnelRow after) {
        return false;
    }

    /**
     * Serialize an update pair to one Kafka {@link ProducerRecord}. The record takes its topic,
     * partition, key and headers from the after row.
     *
     * @param before the {@code UPDATE_BEFORE} row
     * @param after the {@code UPDATE_AFTER} row
     * @return kafka record.
     */
    default ProducerRecord<K, V> serializeUpdate(SeaTunnelRow before, SeaTunnelRow after) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support merging update events");
    }
}
