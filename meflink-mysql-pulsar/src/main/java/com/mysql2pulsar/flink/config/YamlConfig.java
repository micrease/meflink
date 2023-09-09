package com.mysql2pulsar.flink.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
        Mysql2Pulsar mysql2Kafka = loadMsql2Kafka(propertiesFilePath);
        config.setMysql2Pulsar(mysql2Kafka);
        return config;
    }

    public static Mysql2Pulsar loadMsql2Kafka(String configPath) throws IOException {
        if (StringUtils.isEmpty(configPath)) {
            configPath = getResourcePath("mysql2pulsar.yaml");
        }
        Mysql2Pulsar mysql2Pulsar = new Mysql2Pulsar();
        Map map = loadYamlConf(configPath);
        LinkedHashMap<String, Object> job = (LinkedHashMap<String, Object>) map.get("job");
        mysql2Pulsar.setJobID(job.getOrDefault("id", "mk01").toString());
        mysql2Pulsar.setJobCheckpointDirectory(job.getOrDefault("checkpoint-directory", "file:///tmp/checkpoint").toString());

        LinkedHashMap<String, Object> datasource = (LinkedHashMap<String, Object>) map.get("datasource");
        LinkedHashMap<String, Object> mysql = (LinkedHashMap<String, Object>) datasource.get("mysql");

        mysql2Pulsar.setDatasourceMysqlHost(mysql.getOrDefault("host", "127.0.0.1").toString());
        mysql2Pulsar.setDatasourceMysqlPort((int) mysql.getOrDefault("port", 3306));
        mysql2Pulsar.setDatasourceMysqlUsername(mysql.getOrDefault("username", "root").toString());
        mysql2Pulsar.setDatasourceMysqlPassword(mysql.getOrDefault("password", "123456").toString());
        mysql2Pulsar.setDatasourceMysqlSlaveId((int) mysql.getOrDefault("slave_id", 1000));
        mysql2Pulsar.setDatasourceMysqlTimezone(mysql.getOrDefault("timezone", "Asia/Shanghai").toString());

        mysql2Pulsar.setStartupMode((int) mysql.getOrDefault("startup_mode", 0));
        mysql2Pulsar.setStartupTimestamp((int) mysql.getOrDefault("startup_timestamp", 0));
        mysql2Pulsar.setDatasourceDatabases(mysql.getOrDefault("databases", "").toString());
        mysql2Pulsar.setDatasourceTables(mysql.getOrDefault("tables", "").toString());

        //sink
        LinkedHashMap<String, Object> sink = (LinkedHashMap<String, Object>) map.get("sink");
        LinkedHashMap<String, Object> sinkKafka = (LinkedHashMap<String, Object>) sink.get("pulsar");

        mysql2Pulsar.setSinkServiceUrl(sinkKafka.getOrDefault("service-url", "http://127.0.0.1:6650").toString());
        mysql2Pulsar.setSinkAdminUrl(sinkKafka.getOrDefault("admin-url", "http://127.0.0.1:8080").toString());
        logger.info("{}", map);
        return mysql2Pulsar;
    }
}
