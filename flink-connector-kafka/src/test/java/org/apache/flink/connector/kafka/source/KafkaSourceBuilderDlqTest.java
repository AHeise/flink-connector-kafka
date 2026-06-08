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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.ErrorOutputTag;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link KafkaSourceBuilder} threads the deserialization-error side output to the source. */
class KafkaSourceBuilderDlqTest {

    @Test
    void builderWiresDeserializationErrorTagToSource() {
        final OutputTag<ConsumerRecord<byte[], byte[]>> dlq =
                new ErrorOutputTag<>(
                        "kafka-dlq",
                        TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {}));

        final KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("topic")
                        .setGroupId("group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setDeserializationErrorTag(dlq)
                        .build();

        assertThat(source.getDeserializationErrorTag()).isEqualTo(dlq);
    }
}
