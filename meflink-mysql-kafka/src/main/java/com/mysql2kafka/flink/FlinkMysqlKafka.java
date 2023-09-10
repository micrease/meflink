
package com.mysql2kafka.flink;

import com.alibaba.fastjson.JSONObject;
import com.mysql2kafka.flink.config.Config;
import com.mysql2kafka.flink.config.Mysql2Kafka;
import com.mysql2kafka.flink.config.YamlConfig;
import com.mysql2kafka.flink.kafka.FlinkRebalancePartitioner;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FlinkMysqlKafka {

    private static final Logger logger = LoggerFactory.getLogger(FlinkMysqlKafka.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Config config = YamlConfig.loadConfig(env, args);
        //Caused by: org.apache.flink.table.api.ValidationException: The MySQL server has a timezone offset (0 seconds ahead of UTC)
        // which does not match the configured timezone Asia/Shanghai. Specify the right server-time-zone to avoid inconsistencies for time-related fields.
        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "double");
        properties.put("bigint.unsigned.handling.mode", "long");
        properties.setProperty("converters", "dateConverters");
        properties.setProperty("dateConverters.type", "com.common.meflink.utils.MySqlDateTimeConverter");

        Mysql2Kafka conf = config.getMysql2Kafka();
        //默认增量订阅
        StartupOptions startupOptions = StartupOptions.latest();
        String serverId = "2000";
        if (conf.getStartupMode() == StartupMode.INITIAL.ordinal()) {
            startupOptions = StartupOptions.initial();
            serverId = "3000";
        } else if (conf.getStartupMode() == StartupMode.TIMESTAMP.ordinal()) {
            if (conf.getStartupTimestamp() == 0) {
                throw new Exception("StartupMode is TIMESTAMP,But not setting startup_timestamp");
            }
            startupOptions = StartupOptions.timestamp(conf.getStartupTimestamp());
        }

        if (conf.getDatasourceMysqlSlaveId() > 0) {
            serverId = String.valueOf(conf.getDatasourceMysqlSlaveId());
        }

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(conf.getDatasourceMysqlHost())
                .serverTimeZone(conf.getDatasourceMysqlTimezone())
                .port(conf.getDatasourceMysqlPort())
                .databaseList(conf.getDatasourceDatabases()) // set captured database
                .tableList(conf.getDatasourceTables()) // set captured table
                .username(conf.getDatasourceMysqlUsername()).password(conf.getDatasourceMysqlPassword()).serverId(serverId)
                .debeziumProperties(properties).startupOptions(startupOptions).deserializer(new JsonDebeziumDeserializationSchema()).includeSchemaChanges(true) // converts SourceRecord to JSON String
                .build();


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers(conf.getSinkKafkaBrokers()).
                setProperty("transaction.timeout.ms", "600000").setRecordSerializer(getKafkaRecordSerializer()).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(3000);
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(backend);

        if (!StringUtils.isEmpty(conf.getJobCheckpointDirectory())) {
            env.getCheckpointConfig().setCheckpointStorage(conf.getJobCheckpointDirectory());
        }
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .uid(conf.getJobID()).setParallelism(1).sinkTo(kafkaSink).setParallelism(conf.getSinkKafkaParallelism());
        env.execute("MySQL Binlog -> Kafka:" + conf.getJobID());
    }

    private static KafkaRecordSerializationSchema getKafkaRecordSerializer() {
        return KafkaRecordSerializationSchema.builder().setTopicSelector((String element) -> {
            JSONObject rawData = JSONObject.parseObject(element);
            JSONObject source = rawData.getJSONObject("source");
            String db = source.getString("db");
            String table = source.getString("table");

            String topic = "mysqlbinlog_ddl";
            logger.info("收取到消息:" + element);
            if (!StringUtils.isEmpty(db) && !StringUtils.isEmpty(table)) {
                topic = String.format("mysqlbinlog_%s_%s", db, table);
            }
            return topic;
        }).setValueSerializationSchema(new SimpleStringSchema()).setKeySerializationSchema(new SimpleStringSchema()).setPartitioner(new FlinkRebalancePartitioner<>()).build();
    }
}
