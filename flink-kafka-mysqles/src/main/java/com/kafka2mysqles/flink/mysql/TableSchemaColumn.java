package com.kafka2mysqles.flink.mysql;

import lombok.Data;

@Data
public class TableSchemaColumn {
    int columnType;
    String columnTypeName;
    Object defaultESValue;
}