package com.medata.flink.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ElasticsearchSink extends RichSinkFunction<List<JSONObject>> {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSink.class);
    private RestClient restClient;
    private ElasticsearchClient client;
    private ElasticsearchAsyncClient asyncClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("113.142.55.150", 9200));

        RestClient restClient = builder.build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    @Override
    public void close() throws Exception {
        client.shutdown();
        restClient.close();
    }

    @Override
    public void invoke(List<JSONObject> values, Context context) throws Exception {
        //client._transport()
        //https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/indexing-bulk.html
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (JSONObject jsonObject : values) {
            JSONObject doc=jsonObject.getJSONObject("after");
            String id = doc.getString("id");
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index("test11111")
                            .id(id)
                            .document(doc)
                    )
            );
        }

        BulkResponse result = client.bulk(bulkBuilder.build());
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