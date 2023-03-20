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
package com.medata.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.medata.flink.biz.KafkaMessageTransform;
import com.medata.flink.elasticsearch.ElasticsearchSink;
import com.medata.flink.utils.CountTriggerWithTimeout;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FlinkKafkaES {

    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaES.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        try {
            env.enableCheckpointing(10000);
            //topic列表
            String[] topics = new String[]{"mysqlbinlog_test_tb_order"};
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers("127.0.0.1:9092")
                    .setTopics(topics)
                    .setGroupId("my-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            SingleOutputStreamOperator<JSONObject> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").map(new MapFunction<String, JSONObject>() {
                public JSONObject map(String value) throws Exception {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject = JSON.parseObject(value);
                    jsonObject = KafkaMessageTransform.Message(jsonObject);
                    return jsonObject;
                }
            });

            kafkaStream.timeWindowAll(Time.seconds(1)).trigger(new CountTriggerWithTimeout<>(10, TimeCharacteristic.ProcessingTime)).apply(new AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<JSONObject> values, Collector<List<JSONObject>> out) throws Exception {
                    ArrayList<JSONObject> list = Lists.newArrayList(values);
                    if (list.size() > 0) {
                        out.collect(list);
                    }
                }
            }).addSink(new ElasticsearchSink());

            env.execute("kafka2es");
        } catch (Exception e) {
            logger.error("kafka2es fail " + e.getMessage());
        }
    }
}
