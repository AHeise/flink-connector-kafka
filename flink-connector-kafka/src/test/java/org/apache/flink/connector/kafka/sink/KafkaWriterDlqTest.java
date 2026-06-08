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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.util.ErrorOutputTag;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link KafkaWriter} routes records that fail serialization to a DLQ side output. */
class KafkaWriterDlqTest {

    private static final OutputTag<String> DLQ = new ErrorOutputTag<>("sink-dlq", STRING);

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
    void builderWiresSerializationErrorTagToSink() {
        final KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("topic")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setSerializationErrorTag(DLQ)
                        .build();
        assertThat(sink.getSerializationErrorTag()).isEqualTo(DLQ);
    }

    @Test
    void brokenProducerRecordIsRoutedToDlq() throws Exception {
        final KafkaWriter<String> writer = createWriter();
        writer.setSerializationErrorTag(DLQ);
        final RecordingContext context = new RecordingContext();

        writer.write("ok", context); // serialize returns null -> skipped, no DLQ, no producer
        writer.write("bad", context); // serialize throws -> DLQ

        assertThat(context.sideValues).containsExactly("bad");
        assertThat(context.sideTag).isEqualTo(DLQ);
    }

    @Test
    void brokenProducerRecordFailsWhenNoDlqConfigured() throws Exception {
        final KafkaWriter<String> writer = createWriter();
        final RecordingContext context = new RecordingContext();

        assertThatThrownBy(() -> writer.write("bad", context)).isInstanceOf(Exception.class);
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
