package com.kafka2mysqles.flink.mysql;


//org.apache.flink.connector.jdbc.dialect.mysql.MySqlTypeMapper
public class MysqlType {
    private static final String MYSQL_UNKNOWN = "UNKNOWN";
    private static final String MYSQL_BIT = "BIT";
    private static final String MYSQL_TINYINT = "TINYINT";
    private static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String MYSQL_SMALLINT = "SMALLINT";
    private static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    private static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MYSQL_INT = "INT";
    private static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    private static final String MYSQL_INTEGER = "INTEGER";
    private static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String MYSQL_BIGINT = "BIGINT";
    private static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String MYSQL_DECIMAL = "DECIMAL";
    private static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String MYSQL_FLOAT = "FLOAT";
    private static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MYSQL_DOUBLE = "DOUBLE";
    private static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String MYSQL_CHAR = "CHAR";
    private static final String MYSQL_VARCHAR = "VARCHAR";
    private static final String MYSQL_TINYTEXT = "TINYTEXT";
    private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MYSQL_TEXT = "TEXT";
    private static final String MYSQL_LONGTEXT = "LONGTEXT";
    private static final String MYSQL_JSON = "JSON";
    private static final String MYSQL_DATE = "DATE";
    private static final String MYSQL_DATETIME = "DATETIME";
    private static final String MYSQL_TIME = "TIME";
    private static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    private static final String MYSQL_YEAR = "YEAR";
    private static final String MYSQL_TINYBLOB = "TINYBLOB";
    private static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String MYSQL_BLOB = "BLOB";
    private static final String MYSQL_LONGBLOB = "LONGBLOB";
    private static final String MYSQL_BINARY = "BINARY";
    private static final String MYSQL_VARBINARY = "VARBINARY";
    private static final String MYSQL_GEOMETRY = "GEOMETRY";
    private static final int RAW_TIME_LENGTH = 10;
    private static final int RAW_TIMESTAMP_LENGTH = 19;


    public static  boolean isDateTime(String typeName){
        if(typeName==null){
            return false;
        }
        typeName = typeName.toUpperCase();
        if(typeName.equals(MYSQL_DATETIME) || typeName.equals(MYSQL_TIMESTAMP)){
            return true;
        }
        return false;
    }
    public static Object getESDefaultValue(String type) {
        //MYSQL_MEDIUMINT
        switch (type) {
            case MYSQL_INT:
            case MYSQL_BIGINT:
            case MYSQL_INTEGER:
            case MYSQL_INT_UNSIGNED:
            case MYSQL_TINYINT:
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT_UNSIGNED:
                return 0;
            case MYSQL_DECIMAL:
            case MYSQL_DECIMAL_UNSIGNED:
            case MYSQL_FLOAT:
            case MYSQL_FLOAT_UNSIGNED:
            case MYSQL_DOUBLE:
            case MYSQL_DOUBLE_UNSIGNED:
                return 0;
            case MYSQL_CHAR:
            case MYSQL_VARCHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
            case MYSQL_LONGTEXT:
            case MYSQL_JSON:
                return "";
            case MYSQL_DATE:
                return "1970-01-01";
            case MYSQL_TIME:
                return "00:00:00";
            case MYSQL_YEAR:
                return "1970";
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return "1970-01-01T00:00:00+08:00";
        }
        return "";
    }
}
