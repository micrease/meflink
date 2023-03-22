package com.kafka2mysqles.flink.vo;

import lombok.Data;

@Data
public class ConfigVo {
    private String SourceMysqlSlaveID;
    private String SourceMysqlHost;
    private int SourceMysqlPort;
    private String SourceMysqlUsername;
    private String SourceMysqlPassword;
    private String[] SourceMysqlDBList;
    private String[] SourceMysqlTableList;
    private String SourceMysqlTimezone;
    private String CheckpointDirectory;
    private String SourceTransformationUid;
    private int SourceMysqlMode;

    //sink to kafka
    private String SinkKafkaBrokers;
    private int SinkKafkaParallelism;
    private int SinkKafkaPartition;

    private String SourceKafkaBrokers;
    private String SourceKafkaTopics;

    //sink to mysql and split table
    private String SinkMysqlJdbcUrl;
    private String SinkMysqlUsername;
    private String SinkMysqlPassword;

    private boolean SinkMysqlSubtableEnable;
    private String SinkMysqlSubtableIncludeTables;
    private int SinkMysqlSubtableNum;

    //sink to es
    private String SinkEsHost;
    private int SinkEsPort;
    private String SinkEsUsername;
    private String SinkEsPassword;
}
