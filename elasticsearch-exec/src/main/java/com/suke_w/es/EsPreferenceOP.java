package com.suke_w.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

public class EsPreferenceOP {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("pre");
        //searchRequest.preference();
        //searchRequest.preference("_local");
        //searchRequest.preference("_only_local");
        //searchRequest.preference("_only_nodes:jFyX3IB0Tayeea9oyoxi7g,RGYZDkfpQhK13ps5fJG_EA,dh3a_B9OTJWxIzF7D9J_lg");
        searchRequest.preference("_prefer_nodes:jFyX3IB0Tayeea9oyoxi7g");

        //bigadata01:jFyX3IB0Tayeea9oyoxi7g，bigdata02:RGYZDkfpQhK13ps5fJG_EA，bigdata03:dh3a_B9OTJWxIzF7D9J_lg


        SearchResponse res = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = res.getHits();

        System.out.println("查询共返回：" + hits.getTotalHits().value + "条数据");

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }


        client.close();
    }
}
