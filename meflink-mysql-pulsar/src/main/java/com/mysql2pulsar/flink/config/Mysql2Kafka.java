package com.mysql2pulsar.flink.config;

import lombok.Data;

@Data
public class Mysql2Kafka {
    String JobID;
    String JobCheckpointDirectory;
    //数据源
    String datasourceMysqlHost;
    int datasourceMysqlPort;
    String datasourceMysqlUsername;
    String datasourceMysqlPassword;
    int datasourceMysqlSlaveId;
    String datasourceMysqlTimezone;
    int startupMode;
    long startupTimestamp;
    String datasourceDatabases;
    String datasourceTables;

    String sinkKafkaBrokers;
    int sinkKafkaParallelism;
    int sinkKafkaPartition;
}
