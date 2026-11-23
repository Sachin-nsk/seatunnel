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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "route"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                    });

    private static final String INSERT_1 =
            "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"a\",\"route\":\"r1\"}],\"type\":\"INSERT\",\"database\":null,\"table\":null,\"ts\":null}";
    private static final String DELETE_1 =
            "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"a\",\"route\":\"r1\"}],\"type\":\"DELETE\",\"database\":null,\"table\":null,\"ts\":null}";
    private static final String INSERT_1_UPDATED =
            "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"b\",\"route\":\"r1\"}],\"type\":\"INSERT\",\"database\":null,\"table\":null,\"ts\":null}";
    private static final String UPDATE_1 =
            "{\"old\":[{\"id\":1,\"name\":\"a\",\"route\":\"r1\"}],\"data\":[{\"id\":1,\"name\":\"b\",\"route\":\"r1\"}],\"type\":\"UPDATE\",\"database\":null,\"table\":null,\"ts\":null}";

    @Test
    void testUpdatePairIsSplitWhenMergeIsDisabled() {
        Recorder recorder = new Recorder(options(false));

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "b", "r1"));

        Assertions.assertEquals(Arrays.asList(DELETE_1, INSERT_1_UPDATED), recorder.values());
    }

    @Test
    void testUpdatePairIsMergedIntoOneCanalUpdate() {
        Recorder recorder = new Recorder(options(true));

        recorder.writer.write(row(RowKind.INSERT, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "b", "r1"));
        recorder.writer.write(row(RowKind.DELETE, 1, "b", "r1"));

        List<String> values = recorder.values();
        Assertions.assertEquals(3, values.size());
        Assertions.assertEquals(INSERT_1, values.get(0));
        Assertions.assertEquals(UPDATE_1, values.get(1));
        Assertions.assertTrue(values.get(2).contains("\"type\":\"DELETE\""));
    }

    @Test
    void testUnmatchedRowsKeepTheirStandaloneMessages() {
        Recorder recorder = new Recorder(options(true));

        // UPDATE_BEFORE followed by a row that is not its UPDATE_AFTER is sent as DELETE.
        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.INSERT, 1, "a", "r1"));
        // UPDATE_AFTER without a preceding UPDATE_BEFORE is sent as INSERT, with no stale old.
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "b", "r1"));

        Assertions.assertEquals(
                Arrays.asList(DELETE_1, INSERT_1, INSERT_1_UPDATED), recorder.values());
    }

    @Test
    void testConsecutiveUpdateBeforeRowsAreNotDropped() {
        Recorder recorder = new Recorder(options(true));

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "b", "r1"));

        Assertions.assertEquals(Arrays.asList(DELETE_1, UPDATE_1), recorder.values());
    }

    @Test
    void testUpdatesFromDifferentTablesAreNotMerged() {
        Recorder recorder = new Recorder(options(true));

        SeaTunnelRow before = row(RowKind.UPDATE_BEFORE, 1, "a", "r1");
        before.setTableId("db.t1");
        SeaTunnelRow after = row(RowKind.UPDATE_AFTER, 1, "b", "r1");
        after.setTableId("db.t2");
        recorder.writer.write(before);
        recorder.writer.write(after);

        List<String> values = recorder.values();
        Assertions.assertEquals(2, values.size());
        Assertions.assertTrue(values.get(0).contains("\"type\":\"DELETE\""));
        Assertions.assertTrue(values.get(1).contains("\"type\":\"INSERT\""));
    }

    @Test
    void testKeyChangeIsNotMerged() {
        Map<String, Object> options = options(true);
        options.put("partition_key_fields", Collections.singletonList("id"));
        Recorder recorder = new Recorder(options);

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 2, "a", "r1"));

        List<ProducerRecord<byte[], byte[]>> records = recorder.records();
        Assertions.assertEquals(2, records.size());
        Assertions.assertTrue(value(records.get(0)).contains("\"type\":\"DELETE\""));
        Assertions.assertTrue(key(records.get(0)).contains("\"id\":1"));
        Assertions.assertTrue(value(records.get(1)).contains("\"type\":\"INSERT\""));
        Assertions.assertTrue(key(records.get(1)).contains("\"id\":2"));
    }

    @Test
    void testMergedRecordKeepsTheKeyOfTheInsertRecord() {
        Map<String, Object> mergeOptions = options(true);
        mergeOptions.put("partition_key_fields", Collections.singletonList("id"));
        Recorder merged = new Recorder(mergeOptions);
        Map<String, Object> splitOptions = options(false);
        splitOptions.put("partition_key_fields", Collections.singletonList("id"));
        Recorder split = new Recorder(splitOptions);

        for (Recorder recorder : Arrays.asList(merged, split)) {
            recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
            recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "b", "r1"));
        }

        Assertions.assertEquals(1, merged.records().size());
        Assertions.assertEquals(UPDATE_1, value(merged.records().get(0)));
        Assertions.assertEquals(key(split.records().get(1)), key(merged.records().get(0)));
    }

    @Test
    void testTopicChangeIsNotMerged() {
        Map<String, Object> options = options(true);
        options.put("topic", "${route}");
        Recorder recorder = new Recorder(options);

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.write(row(RowKind.UPDATE_AFTER, 1, "a", "r2"));

        List<ProducerRecord<byte[], byte[]>> records = recorder.records();
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("r1", records.get(0).topic());
        Assertions.assertTrue(value(records.get(0)).contains("\"type\":\"DELETE\""));
        Assertions.assertEquals("r2", records.get(1).topic());
        Assertions.assertTrue(value(records.get(1)).contains("\"type\":\"INSERT\""));
    }

    @Test
    void testPendingUpdateBeforeIsSentBeforeCommitAndClose() throws Exception {
        Recorder recorder = new Recorder(options(true));

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        Assertions.assertTrue(recorder.records().isEmpty());
        recorder.writer.prepareCommit();

        InOrder inOrder = Mockito.inOrder(recorder.sender);
        inOrder.verify(recorder.sender).send(Mockito.any());
        inOrder.verify(recorder.sender).prepareCommit();
        Assertions.assertEquals(Collections.singletonList(DELETE_1), recorder.values());

        recorder.writer.write(row(RowKind.UPDATE_BEFORE, 1, "a", "r1"));
        recorder.writer.close();

        inOrder.verify(recorder.sender).send(Mockito.any());
        inOrder.verify(recorder.sender).close();
        Assertions.assertEquals(Arrays.asList(DELETE_1, DELETE_1), recorder.values());
    }

    private static Map<String, Object> options(boolean mergeUpdateEvent) {
        Map<String, Object> options = new HashMap<>();
        options.put("topic", "test_topic");
        options.put("bootstrap.servers", "localhost:9092");
        options.put("format", "canal_json");
        options.put("merge_update_event", mergeUpdateEvent);
        return options;
    }

    private static SeaTunnelRow row(RowKind kind, int id, String name, String route) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name, route});
        row.setRowKind(kind);
        return row;
    }

    private static String key(ProducerRecord<byte[], byte[]> record) {
        return new String(record.key(), StandardCharsets.UTF_8);
    }

    private static String value(ProducerRecord<byte[], byte[]> record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }

    /** A writer whose sender is a mock that records every sent record. */
    @SuppressWarnings("unchecked")
    private static class Recorder {
        private final KafkaProduceSender<byte[], byte[]> sender =
                Mockito.mock(KafkaProduceSender.class);
        private final KafkaSinkWriter writer;

        Recorder(Map<String, Object> options) {
            writer =
                    new KafkaSinkWriter(
                            Mockito.mock(SinkWriter.Context.class),
                            KafkaSinkSerializer.create(ReadonlyConfig.fromMap(options), ROW_TYPE),
                            sender);
        }

        List<ProducerRecord<byte[], byte[]>> records() {
            ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            Mockito.verify(sender, Mockito.atLeast(0)).send(captor.capture());
            return captor.getAllValues();
        }

        List<String> values() {
            return records().stream().map(KafkaSinkWriterTest::value).collect(Collectors.toList());
        }
    }
}
