
package com.mysql2pulsar.flink;

import com.alibaba.fastjson.JSONObject;
import com.mysql2pulsar.flink.config.Config;
import com.mysql2pulsar.flink.config.Mysql2Pulsar;
import com.mysql2pulsar.flink.config.YamlConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

        Mysql2Pulsar conf = config.getMysql2Pulsar();
        //默认增量订阅
        StartupOptions startupOptions = StartupOptions.latest();
        String serverId = "2001";
        if (conf.getStartupMode() == StartupMode.INITIAL.ordinal()) {
            startupOptions = StartupOptions.initial();
            serverId = "3001";
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


        System.out.println("getSinkAdminUrl=" + conf.getSinkAdminUrl());
        System.out.println("getSinkServiceUrl=" + conf.getSinkServiceUrl());
        PulsarSinkBuilder buider = PulsarSink.builder()
                //http://192.168.1.78:8080
                .setAdminUrl(conf.getSinkAdminUrl())
                //pulsar://192.168.1.78:6650
                .setServiceUrl(conf.getSinkServiceUrl())
                .setSerializationSchema(new SimpleStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);

        buider.setTopicRouter(new CustomTopicRouter(topics));
        PulsarSink<String> pulsarSink = buider.build();


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
                .uid(conf.getJobID()).setParallelism(1).sinkTo(pulsarSink).setParallelism(1);
        env.execute("MySQL Binlog -> Pulsar:" + conf.getJobID());
    }

    public static class CustomTopicRouter implements TopicRouter<String> {
        private static final long serialVersionUID = 1698701183626468094L;

        private final List<String> topics;

        public CustomTopicRouter(List<String> topics) {
            this.topics = topics;
        }

        @Override
        public TopicPartition route(String row, String key, List<TopicPartition> partitions, PulsarSinkContext context) {
            //row={"before":{"id":1,"order_no":"2222","third_order_no":"4324234","user_id":222,"inviter_id":22,"shop_id":222,"goods_id":0,"goods_cate_id":0,"goods_name":"","price":0.0,"amount":0,"total_price":0.0,"created_time":null,"coupon_name":"","pay_channel":0,"coupon_id":0,"status":0,"user_remark":"","freight_charge":0.0,"pay_amount":0.0,"pay_order_no":"","address_id":0,"pay_time":null,"updated_time":null,"send_out_time":null,"finished_time":null,"coupon_amount":0.0,"order_month":0,"change_time":"2023-09-08 15:23:03","create_date":""},
            // "after":{"id":1,"order_no":"2222","third_order_no":"4324234","user_id":3333,"inviter_id":22,"shop_id":222,"goods_id":0,"goods_cate_id":0,"goods_name":"","price":0.0,"amount":0,"total_price":0.0,"created_time":null,"coupon_name":"","pay_channel":0,"coupon_id":0,"status":0,"user_remark":"","freight_charge":0.0,"pay_amount":0.0,"pay_order_no":"","address_id":0,"pay_time":null,"updated_time":null,"send_out_time":null,"finished_time":null,"coupon_amount":0.0,"order_month":0,"change_time":"2023-09-08 15:24:59","create_date":""},
            // "source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1694186699000,"snapshot":"false","db":"test","sequence":null,"table":"tb_order","server_id":1,"gtid":null,"file":"孤火-bin.000005","pos":909,"row":0,"thread":null,"query":null},
            // "op":"u","ts_ms":1694186703443,"transaction":null}
            JSONObject rawData = JSONObject.parseObject(row);
            JSONObject source = rawData.getJSONObject("source");
            String db = source.getString("db");
            String table = source.getString("table");

            String topic = "mysqlbinlog_ddl";
            logger.info("收取到消息:" + row);
            if (!StringUtils.isEmpty(db) && !StringUtils.isEmpty(table)) {
                topic = String.format("mysqlbinlog_%s_%s", db, table);
            }
            System.out.println("topic====" + topic);
            return new TopicPartition(topic);
        }
    }
}