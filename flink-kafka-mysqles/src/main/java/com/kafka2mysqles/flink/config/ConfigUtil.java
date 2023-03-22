package com.kafka2mysqles.flink.config;

import com.kafka2mysqles.flink.vo.ConfigVo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ConfigUtil {

    //全量模式
    public static final int SOURCE_MODE_FULL_DATA = 1;
    //增量模式
    public static final int SOURCE_MODE_LATEST_INCR = 0;
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    public static ConfigVo loadConfig(StreamExecutionEnvironment env, String[] args) throws Exception {
        ConfigVo configVo = new ConfigVo();

        //-config_path /root/flink-1.16.1/conf.d/application.properties
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propertiesFilePath = parameterTool.get("config_path");
        if (StringUtils.isEmpty(propertiesFilePath)) {
            String defaultPath = ClassLoader.getSystemClassLoader().getResource("application.properties").getFile();
            File file = new File(defaultPath);
            propertiesFilePath = file.getAbsolutePath();
        }

        logger.info(">>>>>>start to load properties from {}", propertiesFilePath);
        parameterTool = ParameterTool.fromPropertiesFile(propertiesFilePath);
        String sourceMysqlHost = parameterTool.get("source.mysql.host", "127.0.0.1");
        int sourceMysqlPort = parameterTool.getInt("source.mysql.port", 3306);
        int sourceMysqlMode = parameterTool.getInt("source.mysql.mode", 0);
        String sourceTransformationUid = parameterTool.get("source.transformation.uid", "mysql-uid-111111");//job unique id
        String sourceMysqlSlaveId = parameterTool.getRequired("source.mysql.slave_id");//job unique id
        String sourceMysqlUsername = parameterTool.get("source.mysql.username", "root");
        String sourceMysqlPassword = parameterTool.get("source.mysql.password", "123456");
        String sourceMysqlDBList = parameterTool.get("source.mysql.db_list", "test");
        String sourceMysqlTableList = parameterTool.get("source.mysql.table_list", "tb_order");
        String sourceMysqlTimezone = parameterTool.get("source.mysql.timezone", "Asia/Shanghai");
        String[] dbListArr = StringUtils.split(sourceMysqlDBList, ",");
        String[] tableListArr = StringUtils.split(sourceMysqlTableList, ",");
        String sinkKafkaBrokers = parameterTool.get("sink.kafka.brokers", "10.7.120.105:9092");
        int sinkKafkaParallelism = parameterTool.getInt("sink.kafka.parallelism", 1);
        int sinkKafkaPartition = parameterTool.getInt("sink.kafka.partition", 1);
        String checkPointDirectory = parameterTool.get("checkpoint.directory", "file:///tmp/checkpoint");

        logger.info("args:" + parameterTool.getProperties().toString());
        if (StringUtils.isEmpty(sourceMysqlHost) || StringUtils.isEmpty(sourceMysqlDBList) || StringUtils.isEmpty(sourceMysqlTableList) || StringUtils.isEmpty(sourceMysqlUsername)) {
            throw new Exception("Source Mysql Config Error:" + parameterTool.getProperties().toString());
        }

        if (StringUtils.isEmpty(sinkKafkaBrokers)) {
            throw new Exception("Sink Kafka Broker Config Error:" + parameterTool.getProperties().toString());
        }
        configVo.setSourceMysqlHost(sourceMysqlHost);
        configVo.setSourceMysqlPort(sourceMysqlPort);
        configVo.setSourceMysqlUsername(sourceMysqlUsername);
        configVo.setSourceMysqlPassword(sourceMysqlPassword);
        configVo.setSourceMysqlTimezone(sourceMysqlTimezone);
        configVo.setSourceMysqlSlaveID(sourceMysqlSlaveId);
        configVo.setSourceMysqlDBList(dbListArr);
        configVo.setSourceMysqlTableList(tableListArr);
        configVo.setSourceTransformationUid(sourceTransformationUid);
        configVo.setSourceMysqlMode(sourceMysqlMode);
        configVo.setSinkKafkaBrokers(sinkKafkaBrokers);
        configVo.setSinkKafkaParallelism(sinkKafkaParallelism);
        configVo.setCheckpointDirectory(checkPointDirectory);
        configVo.setSinkKafkaPartition(sinkKafkaPartition);

        configVo.setSourceKafkaBrokers(parameterTool.get("source.kafka.brokers", ""));
        configVo.setSourceKafkaTopics(parameterTool.get("source.kafka.topics", ""));

        //sink mysql
        configVo.setSinkMysqlJdbcUrl(parameterTool.get("sink.mysql.jdbc.url", ""));
        configVo.setSinkMysqlUsername(parameterTool.get("sink.mysql.username", "root"));
        configVo.setSinkMysqlPassword(parameterTool.get("sink.mysql.password", "123456"));
        configVo.setSinkMysqlSubtableEnable(parameterTool.getBoolean("sink.mysql.subtable.enable", true));
        configVo.setSinkMysqlSubtableIncludeTables(parameterTool.get("sink.mysql.subtable.includeTables", ""));
        configVo.setSinkMysqlSubtableNum(parameterTool.getInt("sink.mysql.subtable.num", 4));

        configVo.setSinkEsHost(parameterTool.get("sink.es.host", ""));
        configVo.setSinkEsPort(parameterTool.getInt("sink.es.port", 9200));
        configVo.setSinkEsUsername(parameterTool.get("sink.es.username", ""));
        configVo.setSinkEsPassword(parameterTool.get("sink.es.password", ""));
        return configVo;
    }
}
