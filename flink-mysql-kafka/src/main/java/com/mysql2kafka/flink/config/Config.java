package com.mysql2kafka.flink.config;

import lombok.Data;

@Data
public class Config {
    public static final int SOURCE_MODE_FULL_DATA = 1;

    Kafka2MyES kafka2MyES;

    Mysql2Kafka mysql2Kafka;
}
