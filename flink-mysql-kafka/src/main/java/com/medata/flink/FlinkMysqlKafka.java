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

import com.alibaba.fastjson.JSONObject;
import com.medata.flink.config.ConfigUtil;
import com.medata.flink.kafka.FlinkRebalancePartitioner;
import com.medata.flink.vo.ConfigVo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

public class FlinkMysqlKafka {

    private static final Logger logger = LoggerFactory.getLogger(FlinkMysqlKafka.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ConfigVo configVo = ConfigUtil.loadConfig(env,args);
        //Caused by: org.apache.flink.table.api.ValidationException: The MySQL server has a timezone offset (0 seconds ahead of UTC)
        // which does not match the configured timezone Asia/Shanghai. Specify the right server-time-zone to avoid inconsistencies for time-related fields.
        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "double");
        properties.put("bigint.unsigned.handling.mode", "long");

        //默认增量订阅
        StartupOptions startupOptions = StartupOptions.latest();
        String serverId = "2000";
        if (configVo.getSourceMysqlMode() == ConfigUtil.SOURCE_MODE_FULL_DATA) {
            startupOptions = StartupOptions.initial();
            serverId = "3000";
        }

        if(!StringUtils.isEmpty(configVo.getSourceMysqlSlaveID())){
            serverId = configVo.getSourceMysqlSlaveID();
        }

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(configVo.getSourceMysqlHost())
                .serverTimeZone(configVo.getSourceMysqlTimezone())
                .port(configVo.getSourceMysqlPort())
                .databaseList(configVo.getSourceMysqlDBList()) // set captured database
                .tableList(configVo.getSourceMysqlTableList()) // set captured table
                .username(configVo.getSourceMysqlUsername()).password(configVo.getSourceMysqlPassword()).serverId(serverId)
                .debeziumProperties(properties).startupOptions(startupOptions).deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers(configVo.getSinkKafkaBrokers()).
                setProperty("transaction.timeout.ms", "600000").setRecordSerializer(getKafkaRecordSerializer()).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(3000);
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(backend);

        if (!StringUtils.isEmpty(configVo.getCheckpointDirectory())) {
            env.getCheckpointConfig().setCheckpointStorage(configVo.getCheckpointDirectory());
        }
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .uid(configVo.getSourceTransformationUid()).setParallelism(1).sinkTo(kafkaSink).setParallelism(configVo.getSinkKafkaParallelism());
        env.execute("MySQL Binlog -> Kafka:" + configVo.getSourceTransformationUid());
    }

    private static KafkaRecordSerializationSchema getKafkaRecordSerializer() {
        return KafkaRecordSerializationSchema.builder().setTopicSelector((String element) -> {
            JSONObject rawData = JSONObject.parseObject(element);
            JSONObject source = rawData.getJSONObject("source");
            String db = source.getString("db");
            String table = source.getString("table");
            String topic = String.format("mysqlbinlog_%s_%s", db, table);
            //logger.info("收取到消息:" + element);
            return topic;
        }).setValueSerializationSchema(new SimpleStringSchema()).setKeySerializationSchema(new SimpleStringSchema()).setPartitioner(new FlinkRebalancePartitioner<>()).build();
    }
}
