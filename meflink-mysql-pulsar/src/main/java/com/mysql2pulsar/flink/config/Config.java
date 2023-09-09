package com.mysql2pulsar.flink.config;

import lombok.Data;

@Data
public class Config {
    public static final int SOURCE_MODE_FULL_DATA = 1;

    Mysql2Pulsar mysql2Pulsar;
}
