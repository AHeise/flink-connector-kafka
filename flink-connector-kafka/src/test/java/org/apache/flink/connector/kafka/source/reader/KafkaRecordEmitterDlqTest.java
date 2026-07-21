/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link KafkaRecordEmitter}: good records reach the main output; a record that fails
 * deserialization throws (SourceReaderBase routes it to a dead-letter output if one is connected).
 */
class KafkaRecordEmitterDlqTest {

    /** Deserializes UTF-8 strings but fails on the value {@code "bad"}. */
    private static final KafkaRecordDeserializationSchema<String> SCHEMA =
            new KafkaRecordDeserializationSchema<String>() {
                @Override
                public void deserialize(
                        ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
                    final String value = new String(record.value(), StandardCharsets.UTF_8);
                    if (value.equals("bad")) {
                        throw new RuntimeException("cannot deserialize");
                    }
                    out.collect(value);
                }

                @Override
                public TypeInformation<String> getProducedType() {
                    return Types.STRING;
                }
            };

    @Test
    void goodRecordReachesMainOutputAndAdvancesOffset() throws Exception {
        final KafkaRecordEmitter<String> emitter = new KafkaRecordEmitter<>(SCHEMA);
        final KafkaPartitionSplitState state =
                new KafkaPartitionSplitState(
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 0L));
        final RecordingSourceOutput output = new RecordingSourceOutput();

        emitter.emitRecord(record(0L, "good"), output, state);

        assertThat(output.mainValues).containsExactly("good");
        assertThat(state.getCurrentOffset()).isEqualTo(1L);
    }

    @Test
    void brokenConsumerRecordThrows() {
        final KafkaRecordEmitter<String> emitter = new KafkaRecordEmitter<>(SCHEMA);
        final KafkaPartitionSplitState state =
                new KafkaPartitionSplitState(
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 0L));

        assertThatThrownBy(
                        () -> emitter.emitRecord(record(0L, "bad"), new RecordingSourceOutput(), state))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, String value) {
        return new ConsumerRecord<>(
                "topic", 0, offset, null, value.getBytes(StandardCharsets.UTF_8));
    }

    private static final class RecordingSourceOutput implements SourceOutput<String> {
        private final List<String> mainValues = new ArrayList<>();

        @Override
        public void collect(String record) {
            mainValues.add(record);
        }

        @Override
        public void collect(String record, long timestamp) {
            mainValues.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
