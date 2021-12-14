package com.suke_w.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

public class EsSearchOp {
    public static void main(String[] args) throws IOException {
        //获取RestClinet连接
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("user");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("name");
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","tom"));
        //searchSourceBuilder.query(QueryBuilders.matchQuery("age","17"));
        //searchSourceBuilder.query(QueryBuilders.wildcardQuery("name","jo*"));
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).to(20));
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).lt(20));
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).lt(null));
        //searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name","tom")).must(QueryBuilders.matchQuery("age",20)).should(QueryBuilders.matchQuery("age",19)));
        //searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name","zhangsan")).should(QueryBuilders.matchQuery("age",88)));
        //searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name","tom").boost(1.0f)).should(QueryBuilders.matchQuery("age",19).boost(5.0f)));
        //searchSourceBuilder.query(QueryBuilders.multiMatchQuery("tom","name","tag"));
        searchSourceBuilder.query(QueryBuilders.matchQuery("name.keyword","刘德华"));
        //searchSourceBuilder.query(QueryBuilders.termQuery("name","刘德华"));
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","刘德华").operator(Operator.AND));
        //searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name","刘")).must(QueryBuilders.matchQuery("name","德")).must(QueryBuilders.matchQuery("name","华")));
        //highlightBuilder.postTags("</font>");
        //highlightBuilder.preTags("<font color='red'>");

        //searchSourceBuilder.highlighter(highlightBuilder);
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","tom"));
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(15);
        //searchSourceBuilder.sort("age", SortOrder.DESC);
        //searchSourceBuilder.sort("name.keyword", SortOrder.ASC);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        long hitsNum = hits.getTotalHits().value;
        System.out.println("数据总量：" + hitsNum);
        SearchHit[] hitsCollects = hits.getHits();
        for (SearchHit hitsCollect : hitsCollects) {
            String source = hitsCollect.getSourceAsString();
            System.out.println(source);
            //Map<String, Object> sourceAsMap = hitsCollect.getSourceAsMap();
            //String name = sourceAsMap.get("name").toString();
            //int age = Integer.parseInt(sourceAsMap.get("age").toString());
            //Map<String, HighlightField> highlightFields = hitsCollect.getHighlightFields();
            //System.out.println(highlightFields);
            //HighlightField highlightField = highlightFields.get("name");
            //if (highlightField != null) {
            //    Text[] fragments = highlightField.getFragments();
            //    name = "";
            //    for (Text fragment : fragments) {
            //        name += fragment;
            //    }
            //}
            //System.out.println(name + "----" + age);
        }


        //关闭连接
        client.close();
    }
}
