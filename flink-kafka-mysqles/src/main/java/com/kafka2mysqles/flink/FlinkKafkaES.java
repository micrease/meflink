
package com.kafka2mysqles.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.kafka2mysqles.flink.config.Config;
import com.kafka2mysqles.flink.config.YamlConfig;
import com.kafka2mysqles.flink.sink.ElasticMysqlSink;
import com.kafka2mysqles.flink.sink.KafkaMessageTransform;
import com.kafka2mysqles.flink.sink.SinkToES;
import com.kafka2mysqles.flink.utils.CountTriggerWithTimeout;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
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

import java.io.IOException;
import java.util.*;


public class FlinkKafkaES {

    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaES.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Config config = YamlConfig.loadConfig(env, args);

        Configuration conf = new Configuration();
        conf.setString("config", JSON.toJSONString(config));
        env.getConfig().setGlobalJobParameters(conf);

        try {
            env.enableCheckpointing(10000);
            //topic列表
            String[] topics = config.getKafka2MyES().getDatasourceKafkaTopics().split(",");
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(config.getKafka2MyES().getDatasourceKafkaBrokers())
                    .setTopics(topics)
                    .setGroupId(config.getKafka2MyES().getDatasourceKafkaGroupId())
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
            }).setParallelism(2);

            kafkaStream.timeWindowAll(Time.seconds(1)).trigger(new CountTriggerWithTimeout<>(10, TimeCharacteristic.ProcessingTime)).apply(new AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<JSONObject> values, Collector<List<JSONObject>> out) throws Exception {
                    ArrayList<JSONObject> list = Lists.newArrayList(values);
                    logger.info("timeWindowAll Count={}", list.size());
                    if (list.size() > 0) {
                        out.collect(list);
                    }
                }
            }).name("timeWindow").addSink(new ElasticMysqlSink()).setParallelism(4);

            env.execute("kafka2es");
        } catch (Exception e) {
            logger.error("kafka2es fail " + e.getMessage());
        }
    }
}
