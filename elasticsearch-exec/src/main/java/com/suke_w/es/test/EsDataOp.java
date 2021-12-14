package com.suke_w.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;

public class EsDataOp {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        //addData(client);

        //addDataByHashMap(client);



        client.close();
    }

    private static void addDataByHashMap(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("9");
        HashMap<String, Object> hashData = new HashMap<String, Object>();
        hashData.put("name","lisi");
        hashData.put("age",55);
        request.source(hashData);
        client.index(request, RequestOptions.DEFAULT);
    }

    private static void addData(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("8");
        String sourceData = "{" +
                "\"name\":\"zhangsan\","
                + "\"age\":32" +
                "}";
        request.source(sourceData, XContentType.JSON);

        client.index(request, RequestOptions.DEFAULT);
    }
}
