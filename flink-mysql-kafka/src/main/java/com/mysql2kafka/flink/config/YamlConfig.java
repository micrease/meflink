package com.mysql2kafka.flink.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class YamlConfig {
    private static final Logger logger = LoggerFactory.getLogger(YamlConfig.class);

    public static String getResourcePath(String fileName) {
        String defaultPath = "/etc/flink/conf.d/" + fileName;
        File file = new File(defaultPath);
        boolean exist = file.exists();
        if (exist) {
            return file.getAbsolutePath();
        }
        defaultPath = ClassLoader.getSystemClassLoader().getResource(fileName).getFile();
        file = new File(defaultPath);
        String propertiesFilePath = file.getAbsolutePath();
        exist = file.exists();
        if (exist) {
            return file.getAbsolutePath();
        }
        return "";
    }

    public static Map loadYamlConf(String filename) throws IOException {
        // filename = getResourcePath(filename);
        Map conf = new Yaml().load(new FileInputStream(new File(filename)));
        return conf;
    }

    public static Config loadConfig(StreamExecutionEnvironment env, String[] args) throws IOException {
        //-config_path /root/flink-1.16.1/conf.d/application.properties
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propertiesFilePath = parameterTool.get("config_path");
        Config config = new Config();
        Kafka2MyES kafka2MyES = loadKafka2Es(propertiesFilePath);
        Mysql2Kafka mysql2Kafka = loadMsql2Kafka(propertiesFilePath);
        config.setMysql2Kafka(mysql2Kafka);
        config.setKafka2MyES(kafka2MyES);
        return config;
    }

    public static Kafka2MyES loadKafka2Es(String configPath) throws IOException {
        if (StringUtils.isEmpty(configPath)) {
            configPath = getResourcePath("kafka2myes.yaml");
        }
        Kafka2MyES kafka2MyES = new Kafka2MyES();
        Map map = loadYamlConf(configPath);

        LinkedHashMap<String, Object> job = (LinkedHashMap<String, Object>) map.get("job");
        kafka2MyES.setJobID(job.getOrDefault("id", "km01").toString());
        kafka2MyES.setJobCheckpointDirectory(job.getOrDefault("checkpoint-directory", "file:///tmp/checkpoint").toString());


        LinkedHashMap<String, Object> datasource = (LinkedHashMap<String, Object>) map.get("datasource");
        LinkedHashMap<String, Object> kafka = (LinkedHashMap<String, Object>) datasource.get("kafka");

        kafka2MyES.setDatasourceKafkaBrokers(kafka.getOrDefault("brokers", "127.0.0.1").toString());
        kafka2MyES.setDatasourceKafkaTopics(kafka.getOrDefault("topics", "").toString());
        kafka2MyES.setDatasourceKafkaGroupId(kafka.getOrDefault("group-id", "kafka2myes").toString());

        LinkedHashMap<String, Object> sink = (LinkedHashMap<String, Object>) map.get("sink");
        LinkedHashMap<String, Object> sinkMysql = (LinkedHashMap<String, Object>) sink.get("mysql");

        kafka2MyES.setSinkMysqlJdbcUrl(sinkMysql.getOrDefault("jdbc-url", "").toString());
        kafka2MyES.setSinkMysqlUsername(sinkMysql.getOrDefault("username", "root").toString());
        kafka2MyES.setSinkMysqlPassword(sinkMysql.getOrDefault("password", "123456").toString());

        //mysqlSharding
        ArrayList<LinkedHashMap<String, Object>> sinkMysqlShardingRuleList = (ArrayList<LinkedHashMap<String, Object>>) sinkMysql.get("sharding-rule");
        Map<String, Kafka2MyES.MysqlTableSharding> mytableShardingMap = new HashMap<>();
        for (Object sinkMysqlShardingRule : sinkMysqlShardingRuleList) {
            LinkedHashMap<String, Object> sinkMysqlShardingRuleMap = (LinkedHashMap<String, Object>) sinkMysqlShardingRule;
            for (Map.Entry<String, Object> listEntry : sinkMysqlShardingRuleMap.entrySet()) {
                System.out.println(listEntry.getKey() + " : " + listEntry.getValue());
                LinkedHashMap<String, Object> tableShardingMap = (LinkedHashMap<String, Object>) listEntry.getValue();
                Kafka2MyES.MysqlTableSharding tableSharding = new Kafka2MyES.MysqlTableSharding();
                tableSharding.setShardingColumn(tableShardingMap.getOrDefault("sharding-column", "").toString());
                tableSharding.setNumber((int) tableShardingMap.getOrDefault("number", 4));
                mytableShardingMap.put(listEntry.getKey(), tableSharding);
            }
            System.out.println(sinkMysqlShardingRule);
        }

        kafka2MyES.setSinkMysqlTableShardingRule(mytableShardingMap);
        //esSharding
        LinkedHashMap<String, Object> sinkElasticsearch = (LinkedHashMap<String, Object>) sink.get("elasticsearch");
        //esSharding config
        kafka2MyES.setSinkElasticsearchHost(sinkElasticsearch.getOrDefault("host", "").toString());
        kafka2MyES.setSinkElasticsearchPort((int) sinkElasticsearch.getOrDefault("port", 9200));
        if (sinkElasticsearch.get("username") != null) {
            kafka2MyES.setSinkElasticsearchUsername(sinkElasticsearch.getOrDefault("username", "").toString());
        }

        if (sinkElasticsearch.get("password") != null) {
            kafka2MyES.setSinkElasticsearchPassword(sinkElasticsearch.getOrDefault("password", "").toString());
        }
        kafka2MyES.setSinkElasticsearchShardingTables(sinkElasticsearch.getOrDefault("sharding-tables", "").toString());

        //esSharding
        ArrayList<LinkedHashMap<String, Object>> sinkElasticsearchShardingRuleList = (ArrayList<LinkedHashMap<String, Object>>) sinkElasticsearch.get("sharding-rule");
        Map<String, Kafka2MyES.ElasticsearchSharding> esShardingMap = new HashMap<>();
        for (Object sinkElasticsearchShardingRule : sinkElasticsearchShardingRuleList) {
            LinkedHashMap<String, Object> sinkElasticsearchShardingRuleMap = (LinkedHashMap<String, Object>) sinkElasticsearchShardingRule;
            for (Map.Entry<String, Object> listEntry : sinkElasticsearchShardingRuleMap.entrySet()) {
                LinkedHashMap<String, Object> shardingMap = (LinkedHashMap<String, Object>) listEntry.getValue();
                Kafka2MyES.ElasticsearchSharding esSharding = new Kafka2MyES.ElasticsearchSharding();
                esSharding.setIndexBaseName(shardingMap.getOrDefault("index-name", "").toString());
                esSharding.setNumberOfShards((int) shardingMap.getOrDefault("number", 5));
                esSharding.setRoutingColumn(shardingMap.getOrDefault("routing-column", "user_id").toString());
                esSharding.setIndexMonthColumn(shardingMap.getOrDefault("index-month-column", "created_at").toString());
                esSharding.setTemplateFile(shardingMap.getOrDefault("template-file", "").toString());
                esShardingMap.put(listEntry.getKey(), esSharding);
            }
        }
        kafka2MyES.setSinkElasticsearchShardingRule(esShardingMap);
        logger.info("{}", map);
        return kafka2MyES;
    }


    public static Mysql2Kafka loadMsql2Kafka(String configPath) throws IOException {
        if (StringUtils.isEmpty(configPath)) {
            configPath = getResourcePath("mysql2kafka.yaml");
        }
        Mysql2Kafka mysql2Kafka = new Mysql2Kafka();
        Map map = loadYamlConf(configPath);
        LinkedHashMap<String, Object> job = (LinkedHashMap<String, Object>) map.get("job");
        mysql2Kafka.setJobID(job.getOrDefault("id", "mk01").toString());
        mysql2Kafka.setJobCheckpointDirectory(job.getOrDefault("checkpoint-directory", "file:///tmp/checkpoint").toString());

        LinkedHashMap<String, Object> datasource = (LinkedHashMap<String, Object>) map.get("datasource");
        LinkedHashMap<String, Object> mysql = (LinkedHashMap<String, Object>) datasource.get("mysql");

        mysql2Kafka.setDatasourceMysqlHost(mysql.getOrDefault("host", "127.0.0.1").toString());
        mysql2Kafka.setDatasourceMysqlPort((int) mysql.getOrDefault("port", 3306));
        mysql2Kafka.setDatasourceMysqlUsername(mysql.getOrDefault("username", "root").toString());
        mysql2Kafka.setDatasourceMysqlPassword(mysql.getOrDefault("password", "123456").toString());
        mysql2Kafka.setDatasourceMysqlSlaveId((int) mysql.getOrDefault("slave_id", 1000));
        mysql2Kafka.setDatasourceMysqlTimezone(mysql.getOrDefault("timezone", "Asia/Shanghai").toString());

        LinkedHashMap<String, Object> mysqlSync = (LinkedHashMap<String, Object>) mysql.get("sync");
        mysql2Kafka.setDatasourceMysqlSyncRunMode((int) mysqlSync.getOrDefault("run_mode", 0));
        mysql2Kafka.setDatasourceMysqlSyncDatabases(mysqlSync.getOrDefault("databases", "").toString());
        mysql2Kafka.setDatasourceMysqlSyncTables(mysqlSync.getOrDefault("tables", "").toString());

        //sink
        LinkedHashMap<String, Object> sink = (LinkedHashMap<String, Object>) map.get("sink");
        LinkedHashMap<String, Object> sinkKafka = (LinkedHashMap<String, Object>) sink.get("kafka");

        mysql2Kafka.setSinkKafkaBrokers(sinkKafka.getOrDefault("brokers", "127.0.0.1:9092").toString());
        mysql2Kafka.setSinkKafkaPartition((int) sinkKafka.getOrDefault("partition", 1));
        mysql2Kafka.setSinkKafkaParallelism((int) sinkKafka.getOrDefault("parallelism", 1));
        logger.info("{}", map);
        return mysql2Kafka;
    }
}
