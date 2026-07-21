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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.util.DeadLetter;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link KafkaWriter} forwards records that fail serialization to the blessed sink error
 * side output ({@link DataStreamSink#getErrorSideOutput()}), or rethrows when it is not connected.
 */
class KafkaWriterDlqTest {

    /** Serializes UTF-8 strings but fails on {@code "bad"}; returns null otherwise (skip send). */
    private static final KafkaRecordSerializationSchema<String> SCHEMA =
            new KafkaRecordSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(
                        String element, KafkaSinkContext context, Long timestamp) {
                    if ("bad".equals(element)) {
                        throw new RuntimeException("cannot serialize");
                    }
                    return null;
                }
            };

    @Test
    void brokenProducerRecordIsForwardedToBlessedTag() throws Exception {
        final KafkaWriter<String> writer = createWriter();
        final RecordingContext context = new RecordingContext();

        writer.write("ok", context); // serialize returns null -> skipped, no producer touched
        writer.write("bad", context); // serialize throws -> forwarded to the blessed error tag

        assertThat(context.sideTag.getId()).isEqualTo(DataStreamSink.ERROR_SIDE_OUTPUT_ID);
        assertThat(context.sideValues)
                .singleElement()
                .isInstanceOfSatisfying(
                        DeadLetter.class,
                        dl -> {
                            assertThat(dl.getRecord()).isEqualTo("bad");
                            assertThat(dl.getError()).isNotNull();
                        });
    }

    @Test
    void brokenProducerRecordRethrowsWhenErrorOutputNotConnected() throws Exception {
        final KafkaWriter<String> writer = createWriter();
        // A context whose output(...) is the interface default, which throws (not connected).
        final SinkWriter.Context noSideOutput =
                new SinkWriter.Context() {
                    @Override
                    public long currentWatermark() {
                        return 0L;
                    }

                    @Override
                    public Long timestamp() {
                        return null;
                    }
                };

        assertThatThrownBy(() -> writer.write("bad", noSideOutput))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to serialize");
    }

    private static KafkaWriter<String> createWriter() {
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("flink.disable-metrics", "true");
        final TestSinkInitContext initContext = new TestSinkInitContext();
        return new KafkaWriter<>(
                DeliveryGuarantee.NONE,
                props,
                initContext,
                SCHEMA,
                initContext.asSerializationSchemaInitializationContext());
    }

    private static final class RecordingContext implements SinkWriter.Context {
        private final List<Object> sideValues = new ArrayList<>();
        private OutputTag<?> sideTag;

        @Override
        public long currentWatermark() {
            return 0L;
        }

        @Override
        public Long timestamp() {
            return null;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, @Nullable X value) {
            sideTag = outputTag;
            sideValues.add(value);
        }
    }
}
