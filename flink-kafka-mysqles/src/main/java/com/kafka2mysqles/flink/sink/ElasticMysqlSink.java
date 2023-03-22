package com.kafka2mysqles.flink.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.DateTime;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kafka2mysqles.flink.mysql.ColumnInfo;
import com.kafka2mysqles.flink.mysql.MysqlTableSchema;
import com.kafka2mysqles.flink.mysql.MysqlType;
import com.kafka2mysqles.flink.vo.ConfigVo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticMysqlSink extends RichSinkFunction<List<JSONObject>> {
    private static final Logger logger = LoggerFactory.getLogger(ElasticMysqlSink.class);
    private ElasticsearchClient elasticsearchClient;
    private Connection destMysqlConnection;
    private static final String SubTableSeperator = "####";
    //tb_order##insert-->JSONObject
    private Map<String, List<JSONObject>> subTableOperateMap;
    ConfigVo configVo;
    private Map<String, Map<String, ColumnInfo>> tableColumnTypes;

    DateTimeFormatter rfc3339formatter;

    DateTimeFormatter mysqlFormatter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //解析配置
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globalConfig = (Configuration) globalParams;
        String configVoJson = globalConfig.getString("configVo", "");
        if (!StringUtils.isEmpty(configVoJson)) {
            configVo = JSON.parseObject(configVoJson, ConfigVo.class);
        }
        logger.info("ElasticMysqlSink Config={}", configVoJson);
        loadTableSchema();

        //连接es
        RestClientBuilder builder = RestClient.builder(new HttpHost(configVo.getSinkEsHost(), configVo.getSinkEsPort()));
        // Create the transport with a Jackson mapper
        if (!StringUtils.isEmpty(configVo.getSinkEsUsername())) {
            //https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/connecting.html
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(configVo.getSinkEsUsername(), configVo.getSinkEsPassword()));
            builder.setHttpClientConfigCallback(hc -> hc.setDefaultCredentialsProvider(credentialsProvider));
        }
        RestClient restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        elasticsearchClient = new ElasticsearchClient(transport);

        //连接目标库
        destMysqlConnection = getConnection();
        destMysqlConnection.setAutoCommit(false);
        subTableOperateMap = new HashMap<>();


        rfc3339formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .withZone(ZoneId.of("UTC"));


        mysqlFormatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("UTC"));
    }

    @Override
    public void close() throws Exception {
        if (elasticsearchClient != null) {
            elasticsearchClient.shutdown();
        }

        if (destMysqlConnection != null) {
            destMysqlConnection.close();
        }

        if (subTableOperateMap != null) {
            subTableOperateMap.clear();
        }
    }

    @Override
    public void invoke(List<JSONObject> values, Context context) throws Exception {
        logger.info("ElasticMysqlSink invoke  Count={}",values.size());
        subTableOperateMap.clear();

        writeToMysql(values);
        List<JSONObject> esValues = formatESValue(values);
        SinkToES.writeToEs(elasticsearchClient, esValues);
    }

    public void writeToMysql(List<JSONObject> values) throws SQLException {
        //数据分组
        int subTables = configVo.getSinkMysqlSubtableNum();
        for (JSONObject jsonObject : values) {
            JSONObject after = jsonObject.getJSONObject("after");
            int userId = 0;
            if (after.containsKey("uid")) {
                userId = after.getIntValue("uid");
            } else {
                userId = after.getIntValue("user_id");
            }

            int subTableId = userId % subTables;
            JSONObject source = jsonObject.getJSONObject("source");
            String table = source.getString("table");
            String subTableName = String.format("%s_%d", table, subTableId);
            String op = jsonObject.getString("op");
            String key = String.format("%s%s%s", subTableName, SubTableSeperator, op);
            if (!subTableOperateMap.containsKey(key)) {
                subTableOperateMap.put(key, new ArrayList<JSONObject>());
            }
            subTableOperateMap.get(key).add(after);
        }

        subTableOperateMap.forEach((key, rows) -> {
            String[] keyArr = StringUtils.split(key, SubTableSeperator);
            if (keyArr.length != 2) {
                logger.error("error row,key={},value={}", key, rows);
                return;
            }
            String tableName = keyArr[0];
            String operation = keyArr[1];
            try {
                upsert(operation, tableName, rows);
            } catch (SQLException e) {
                logger.error("writeToMysql Error" + e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    public List<JSONObject> formatESValue(List<JSONObject> rows) {
        List<JSONObject> afterList = new ArrayList<>(rows.size());
        for (int idx = 0; idx < rows.size(); idx++) {
            JSONObject row = rows.get(idx);
            JSONObject originAfter = row.getJSONObject("after");
            JSONObject after = originAfter.clone();
            JSONObject source = row.getJSONObject("source");
            String db = source.getString("db");
            String table = source.getString("table");
            String dbTable = String.format("%s.%s", db, table);

            Map<String, ColumnInfo> columnTypes = tableColumnTypes.get(dbTable);
            for (String key : after.keySet()) {
                if (after.get(key) == null) {
                    logger.info("formatESValue {},{},{}", dbTable, key);
                    Object defaultVal = columnTypes.get(key).getDefaultESValue();
                    after.put(key, defaultVal);
                } else {
                    if (MysqlType.isDateTime(columnTypes.get(key).getColumnTypeName())) {
                        //转为Rfc3339
                        DateTime dateTime = DateTime.of(after.getString(key), mysqlFormatter);
                        String result = rfc3339formatter.format(dateTime.toInstant());
                        after.put(key, result);
                    }
                }
            }
            afterList.add(after);
        }
        return afterList;
    }

    private void upsert(String op, String tableName, List<JSONObject> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        JSONObject firstRow = rows.get(0);
        String[] keys = firstRow.keySet().toArray(new String[0]);
        String sql;
        String val = "";
        try {
            Statement statement = destMysqlConnection.createStatement();
            if (op.equals("c") || op.equals("u")) {
                for (int idx = 0; idx < rows.size(); idx++) {
                    JSONObject row = rows.get(idx);
                    String[] insertVals = new String[keys.length];
                    String[] upset = new String[keys.length];

                    for (int i = 0; i < keys.length; i++) {
                        String key = keys[i];
                        val = row.getString(key);
                        if (val == null) {
                            insertVals[i] = "NULL";
                            upset[i] = String.format("`%s`=NULL", key);
                        } else {
                            insertVals[i] = String.format("'%s'", val);
                            upset[i] = String.format("`%s`='%s'", key, val);
                        }
                    }

                    String insertColumns = StringUtils.join(keys, "`,`");
                    String insertValues = StringUtils.join(insertVals, ",");
                    String upsetValues = StringUtils.join(upset, ",");
                    sql = String.format("INSERT INTO `%s`(`%s`) VALUES (%s) ON DUPLICATE KEY UPDATE %s", tableName, insertColumns, insertValues, upsetValues);
                    //logger.info("SQL:" + sql);
                    statement.addBatch(sql);
                }
            }
            int[] counts = statement.executeBatch();
            logger.info("upsert op={},tableName={},rowsCount={},result={}", op, tableName, rows.size(), counts);
            destMysqlConnection.commit();
            statement.clearBatch();
        } catch (SQLException e) {
            destMysqlConnection.rollback();
            throw new RuntimeException(e);
        }
    }

    private void loadTableSchema() throws SQLException {
        String[] tableList = configVo.getSourceMysqlTableList();
        tableColumnTypes = new HashMap<>();
        String jdbcUrl = "jdbc:mysql://%s:%d/%s";
        for (int i = 0; i < tableList.length; i++) {
            String dbTableName = tableList[i];
            String[] arr = StringUtils.split(dbTableName, ".");
            String dbName = arr[0];
            String tableName = arr[1];
            jdbcUrl = String.format(jdbcUrl, configVo.getSourceMysqlHost(), configVo.getSourceMysqlPort(), dbName);
            MysqlTableSchema table = new MysqlTableSchema();
            table.connection(jdbcUrl, configVo.getSourceMysqlUsername(), configVo.getSourceMysqlPassword());
            Map<String, ColumnInfo> columnTypes = table.getTableColumnDataType(dbName, tableName);


            tableColumnTypes.put(dbTableName, columnTypes);
        }

        logger.info("loadTableSchema tableColumnTypes={}", tableColumnTypes);
    }

    private void flinkJDBCTest() {
        //https://developer.aliyun.com/article/1044573
        String name = "my_catalog";
        String defaultDatabase = "test";
        String username = "root";
        String password = "123456";
        //jdbc:postgresql://localhost:5432/
        String baseUrl = "jdbc:mysql://127.0.0.1:3306/";
        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);

        try {
            CatalogColumnStatistics col = catalog.getTableColumnStatistics(new ObjectPath("test", "tb_order"));
            logger.info("CatalogColumnStatistics.getColumnStatisticsData {}", col.getColumnStatisticsData());
            logger.info("CatalogColumnStatistics.getProperties {}", col.getProperties());


            CatalogBaseTable table = catalog.getTable(new ObjectPath("test", "tb_order"));
            logger.info("CatalogBaseTable:{}", table.toString());
            logger.info("CatalogBaseTable getUnresolvedSchema{}", table.getUnresolvedSchema());
        } catch (TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(configVo.getSinkMysqlJdbcUrl(), configVo.getSinkMysqlUsername(), configVo.getSinkMysqlPassword());
        } catch (Exception e) {
            logger.error("mysql connection has exception , msg = " + e.getMessage());
        }
        return con;
    }

}