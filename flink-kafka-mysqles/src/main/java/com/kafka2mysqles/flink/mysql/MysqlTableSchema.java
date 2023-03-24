package com.kafka2mysqles.flink.mysql;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.dialect.mysql.MySqlTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlTableSchema {
    private static final Logger logger = LoggerFactory.getLogger(MysqlTableSchema.class);

    MySqlTypeMapper dialectTypeMapper;
    Connection conn;

    public TableSchema getTableSchema(String databaseName, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM `%s`.`%s`;", databaseName, tableName));
        ResultSetMetaData resultSetMetaData = ps.getMetaData();
        String[] columnNames = new String[resultSetMetaData.getColumnCount()];
        DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        Optional<UniqueConstraint> primaryKey = this.getPrimaryKey(metaData, databaseName, this.getSchemaName(tablePath), this.getTableName(tablePath));

        String driverVersion = conn.getMetaData().getDriverVersion();
        Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
        Matcher matcher = regexp.matcher(driverVersion);
        driverVersion = matcher.find() ? matcher.group(0) : null;
        dialectTypeMapper = new MySqlTypeMapper(metaData.getDatabaseProductVersion(), driverVersion);

        Map<String, TableSchemaColumn> columnDataTypeMap = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); ++i) {
            columnNames[i - 1] = resultSetMetaData.getColumnName(i);
            types[i - 1] = this.fromJDBCType(new ObjectPath(databaseName, tableName), resultSetMetaData, i);
            if (resultSetMetaData.isNullable(i) == 0) {
                types[i - 1] = (DataType) types[i - 1].notNull();
            }

            TableSchemaColumn columnInfo = new TableSchemaColumn();
            columnInfo.setColumnType(resultSetMetaData.getColumnType(i));
            columnInfo.setColumnTypeName(resultSetMetaData.getColumnTypeName(i));
            Object defaultValue = MysqlType.getESDefaultValue(columnInfo.getColumnTypeName());
            columnInfo.setDefaultESValue(defaultValue);
            columnDataTypeMap.put(columnNames[i - 1], columnInfo);
        }

        Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
        primaryKey.ifPresent((pk) -> {
            schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns());
        });

        Schema schema = schemaBuilder.build();
        Map<String, String> props = new HashMap();
        props.put(FactoryUtil.CONNECTOR.key(), "jdbc");
        CatalogTable catalogTable = CatalogTable.of(schema, (String) null, Lists.newArrayList(), props);

        TableSchema tableSchema = new TableSchema();
        tableSchema.setColumns(columnDataTypeMap);
        tableSchema.setDatabaseName(databaseName);
        tableSchema.setTableName(tableName);
        tableSchema.setCatalogTable(catalogTable);

        String createTableDDL = this.getCreateTable(databaseName, tableName);
        tableSchema.setCreateDDL(createTableDDL);
        return tableSchema;
    }

    public String getCreateTable(String databaseName, String tableName) throws SQLException {
        String sql = String.format("show create table `%s`.`%s`;", databaseName, tableName);
        if (StringUtils.isEmpty(databaseName)) {
            sql = String.format("show create table`%s`;", tableName);
        }

        PreparedStatement preparedStatement2 = conn.prepareStatement(sql);
        ResultSet resultSet2 = preparedStatement2.executeQuery();
        while (resultSet2.next()) {
            //tableName = resultSet2.getString("Table");
            String createTable = resultSet2.getString("Create Table");
            return createTable;
        }
        return "";
    }


    public boolean createTable(String createDDL) throws SQLException {
        String sql = createDDL;
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        //返回值:
        //true if the first result is a ResultSet object; false if the first result is an update count or there is no result
        boolean result = preparedStatement.execute();
        return true;
    }

    protected Optional<UniqueConstraint> getPrimaryKey(DatabaseMetaData metaData, String database, String schema, String table) throws SQLException {
        ResultSet rs = metaData.getPrimaryKeys(database, schema, table);
        Map<Integer, String> keySeqColumnName = new HashMap();
        String pkName = null;

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            pkName = rs.getString("PK_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            Preconditions.checkState(!keySeqColumnName.containsKey(keySeq - 1), "The field(s) of primary key must be from the same table.");
            keySeqColumnName.put(keySeq - 1, columnName);
        }

        logger.info("getPrimaryKey keySeqColumnName = {}", keySeqColumnName);
        List<String> pkFields = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : keySeqColumnName.entrySet()) {
            pkFields.add(entry.getValue());
        }

        if (!pkFields.isEmpty()) {
            pkName = pkName == null ? "pk_" + String.join("_", pkFields) : pkName;
            return Optional.of(UniqueConstraint.primaryKey(pkName, pkFields));
        } else {
            return Optional.empty();
        }
    }

    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        return this.dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    public Connection connection(String jdbcUrl, String username, String password) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        //String url = "jdbc:mysql://127.0.0.1:3306/test?allowMultiQueries=true&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true";
        conn = DriverManager.getConnection(jdbcUrl, username, password);
        return conn;
    }

    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
