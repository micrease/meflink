package com.kafka2mysqles.flink.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.*;
import com.alibaba.fastjson.JSONObject;
import com.kafka2mysqles.flink.config.Config;
import com.kafka2mysqles.flink.config.Kafka2MyES;
import com.kafka2mysqles.flink.config.YamlConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

public class SinkToES {
    private static final Logger logger = LoggerFactory.getLogger(SinkToES.class);

    public static void createTemplate(ElasticsearchClient elasticsearchClient, String templateName, String fileName) throws IOException {
        String path = YamlConfig.getResourcePath(fileName);
        logger.info("createTemplate path={},{},{}", path, templateName, fileName);
        if (StringUtils.isEmpty(path)) {
            return;
        }
        Reader input = new FileReader(path);
        PutTemplateRequest request = new PutTemplateRequest.Builder().name(templateName).withJson(input).build();
        PutTemplateResponse response = elasticsearchClient.indices().putTemplate(request);
        logger.info("PutIndexTemplateResponse,{}", response.toString());
        GetTemplateResponse templateResponse = elasticsearchClient.indices().getTemplate();
        templateResponse.result().forEach((key, val) -> {
            if (key.startsWith(".")) return;
            logger.info("GetTemplateResponse,{}", key);
        });
    }

    public static void writeToEs(Config config, ElasticsearchClient elasticsearchClient, List<JSONObject> values) throws IOException {
        Map<String, Kafka2MyES.ElasticsearchSharding> esRules = config.getKafka2MyES().getSinkElasticsearchShardingRule();
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (JSONObject row : values) {
            JSONObject source = row.getJSONObject("source");
            String db = source.getString("db");
            String table = source.getString("table");
            String dbTable = String.format("%s.%s", db, table);

            String indexName;
            String routingColumn;
            JSONObject doc = row.getJSONObject("after");
            if (esRules.get(dbTable) != null) {
                String indexBaseName = esRules.get(dbTable).getIndexBaseName();
                String monthCol = esRules.get(dbTable).getIndexMonthColumn();
                String monthTime = doc.getString(monthCol);
                //2023-01-01 12:00:00
                String monthStr = StringUtils.substring(monthTime, 0, 7).replaceAll("-", "");
                indexName = String.format("%s_%s", indexBaseName, monthStr);
                routingColumn = esRules.get(dbTable).getRoutingColumn();
            } else {
                routingColumn = "id";
                indexName = dbTable;
            }
            String id = doc.getString("id");
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName).routing(routingColumn).id(id)
                            .document(doc)
                    )
            );
        }

        BulkResponse result = elasticsearchClient.bulk(bulkBuilder.build());
        if (result.errors()) {
            logger.error("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null) {
                    logger.error(item.error().reason());
                }
            }
        }
    }
}
