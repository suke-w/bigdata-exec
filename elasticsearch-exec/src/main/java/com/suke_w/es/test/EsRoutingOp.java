package com.suke_w.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

/**
 * routing路由功能的使用
 */
public class EsRoutingOp {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));


        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("rout");
        //searchRequest.preference("_shards:0");
        searchRequest.routing("class2");



        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits responseHits = searchResponse.getHits();
        System.out.println("数据总量：" + responseHits.getTotalHits().value);
        SearchHit[] hits = responseHits.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }
}
