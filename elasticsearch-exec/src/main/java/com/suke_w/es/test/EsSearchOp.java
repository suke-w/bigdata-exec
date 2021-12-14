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

public class EsSearchOp {
    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        SearchRequest searchRequest = new SearchRequest("user");
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        long hitsNum = hits.getTotalHits().value;
        System.out.println(hitsNum);
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }
}
