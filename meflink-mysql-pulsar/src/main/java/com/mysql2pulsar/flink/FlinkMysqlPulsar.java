
package com.mysql2pulsar.flink;

import com.mysql2pulsar.flink.config.Config;
import com.mysql2pulsar.flink.config.Mysql2Kafka;
import com.mysql2pulsar.flink.config.YamlConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkMysqlPulsar {

    private static final Logger logger = LoggerFactory.getLogger(FlinkMysqlPulsar.class);

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


        List<String> topics = new ArrayList();
        PulsarSink<String> pulsarSink = PulsarSink.builder()
                //pulsar://pulsar.us-west.example.com:6650
                .setServiceUrl(conf.getSinkKafkaBrokers()).setTopicRouter(new CustomTopicRouter(topics))
                //http://my-broker.example.com:8080
                .setAdminUrl("")
                //.setTopics("topic1")
                .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


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
                .uid(conf.getJobID()).setParallelism(1).sinkTo(pulsarSink).setParallelism(conf.getSinkKafkaParallelism());
        env.execute("MySQL Binlog -> Kafka:" + conf.getJobID());
    }

}

public class CustomTopicRouter implements TopicRouter<String> {
    private static final long serialVersionUID = 1698701183626468094L;

    private final List<String> topics;
    private final AtomicInteger counter;

    public CustomTopicRouter(List<String> topics) {
        this.topics = topics;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public String route(String s, String key, List<String> partitions, PulsarSinkContext context) {
        int index = counter.incrementAndGet() / 10 % topics.size();
        String topic = topics.get(index);
        return new TopicPartition(topic, index);
    }
}
