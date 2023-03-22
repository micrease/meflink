package com.kafka2mysqles.flink.mysql;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.kafka2mysqles.flink.vo.ConfigVo;
import org.apache.flink.connector.jdbc.dialect.mysql.MySqlTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlTableSchema {
    private static final Logger logger = LoggerFactory.getLogger(MysqlTableSchema.class);

    MySqlTypeMapper dialectTypeMapper;
    Connection conn;

    public Map<String, ColumnInfo> getTableColumnDataType(String dbName, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM `%s`.`%s`;", dbName, tableName));
        ResultSetMetaData resultSetMetaData = ps.getMetaData();
        String[] columnNames = new String[resultSetMetaData.getColumnCount()];
        DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

        String driverVersion = conn.getMetaData().getDriverVersion();
        Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
        Matcher matcher = regexp.matcher(driverVersion);
        driverVersion = matcher.find() ? matcher.group(0) : null;
        dialectTypeMapper = new MySqlTypeMapper(metaData.getDatabaseProductVersion(), driverVersion);
        Map<String, ColumnInfo> columnDataTypeMap = new HashMap<>();

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); ++i) {
            columnNames[i - 1] = resultSetMetaData.getColumnName(i);
            types[i - 1] = this.fromJDBCType(new ObjectPath(dbName, tableName), resultSetMetaData, i);
            if (resultSetMetaData.isNullable(i) == 0) {
                types[i - 1] = (DataType) types[i - 1].notNull();
            }

            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnType(resultSetMetaData.getColumnType(i));
            columnInfo.setColumnTypeName(resultSetMetaData.getColumnTypeName(i));
            Object defaultValue = MysqlType.getESDefaultValue(columnInfo.getColumnTypeName());
            columnInfo.setDefaultESValue(defaultValue);
            columnDataTypeMap.put(columnNames[i - 1], columnInfo);
        }
        conn.close();
        return columnDataTypeMap;
    }

    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        return this.dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    public Connection connection(String jdbcUrl, String username, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //String url = "jdbc:mysql://127.0.0.1:3306/test?allowMultiQueries=true&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true";
            conn = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (Exception e) {
            logger.error("mysql connection has exception , msg = " + e.getMessage());
        }
        return conn;
    }
}
