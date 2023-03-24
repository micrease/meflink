package com.kafka2mysqles.flink.config;

import lombok.Data;

import java.util.Map;

@Data
public class Kafka2MyES {

    @Data
    public static class MysqlTableSharding {
        int number;
        String shardingColumn;
    }

    @Data
    public static class ElasticsearchSharding {
        //索引基本名称,之所以叫基准名称，是因为除了索引是{indexBaseName}_{month}组成
        String indexBaseName;
        //分片数量
        int numberOfShards;
        //路由字段
        String routingColumn;
        //索引月份字段
        String indexMonthColumn;
        //模板文件
        String templateFile;
    }

    String JobID;

    String JobCheckpointDirectory;

    //数据源
    String datasourceKafkaBrokers;
    String datasourceKafkaTopics;
    String datasourceKafkaGroupId;

    //sink mysql
    String sinkMysqlJdbcUrl;
    String sinkMysqlUsername;
    String sinkMysqlPassword;
    Map<String, MysqlTableSharding> sinkMysqlTableShardingRule;

    String sinkElasticsearchHost;
    int sinkElasticsearchPort;
    String sinkElasticsearchUsername;
    String sinkElasticsearchPassword;
    String sinkElasticsearchShardingTables;
    Map<String, ElasticsearchSharding> sinkElasticsearchShardingRule;
}