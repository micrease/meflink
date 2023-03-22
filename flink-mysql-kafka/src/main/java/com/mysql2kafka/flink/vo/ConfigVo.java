package com.mysql2kafka.flink.vo;

import lombok.Data;

@Data
public class ConfigVo {
    private String SourceMysqlSlaveID;
    private String SourceMysqlHost;
    private int SourceMysqlPort;
    private String SourceMysqlUsername;
    private String SourceMysqlPassword;
    private  String[] SourceMysqlDBList;
    private  String[] SourceMysqlTableList;
    private String SourceMysqlTimezone;
    private String SinkKafkaBrokers;
    private int SinkKafkaParallelism;
    private int SinkKafkaPartition;
    private String CheckpointDirectory;
    private String SourceTransformationUid;
    private int SourceMysqlMode;
}
