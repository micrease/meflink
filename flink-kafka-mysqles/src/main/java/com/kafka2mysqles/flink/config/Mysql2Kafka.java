package com.kafka2mysqles.flink.config;

import lombok.Data;

import java.util.Map;

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

    int datasourceMysqlSyncRunMode;
    String datasourceMysqlSyncDatabases;
    String datasourceMysqlSyncTables;

    String sinkKafkaBrokers;
    int sinkKafkaParallelism;
    int sinkKafkaPartition;
}
