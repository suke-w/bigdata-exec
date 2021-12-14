package com.suke_w.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class EsIndexOp {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        //createIndex(client);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java_test1");
        client.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);

        client.close();
    }

    private static void createIndex(RestHighLevelClient client) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("java_test1");
        createIndexRequest.settings(Settings.builder().put("index.number_of_shards",3));
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }
}
