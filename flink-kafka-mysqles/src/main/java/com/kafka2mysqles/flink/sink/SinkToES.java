package com.kafka2mysqles.flink.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SinkToES {
    private static final Logger logger = LoggerFactory.getLogger(SinkToES.class);


    public static void writeToEs(ElasticsearchClient elasticsearchClient, List<JSONObject> values) throws IOException {
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (JSONObject doc : values) {
            // JSONObject doc = jsonObject;
            String id = doc.getString("id");
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index("test11111")
                            .id(id)
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
