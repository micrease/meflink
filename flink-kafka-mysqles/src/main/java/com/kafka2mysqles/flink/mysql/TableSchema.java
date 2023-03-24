package com.kafka2mysqles.flink.mysql;

import lombok.Data;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.Map;

@Data
public class TableSchema {
    String databaseName;
    String tableName;

    CatalogTable catalogTable;
    Map<String, TableSchemaColumn> Columns;

    String createDDL;
}
