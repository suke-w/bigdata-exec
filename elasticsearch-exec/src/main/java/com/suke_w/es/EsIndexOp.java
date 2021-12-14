package com.suke_w.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * 针对ES中索引库的操作
 * 1.创建索引库
 * 2.删除索引库
 */
public class EsIndexOp {
    public static void main(String[] args) throws IOException {
        //获取RestClinet连接
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));
        //创建索引库
        //createIndex(client);

        //删除索引库
        deleteIndex(client);
        //关闭连接
        client.close();

    }

    /**
     * 删除索引库
     * @param client
     * @throws IOException
     */
    private static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("java_test");
        //执行
        client.indices().delete(deleteRequest, RequestOptions.DEFAULT);
    }

    /**
     * 创建索引库
     * @param client
     * @throws IOException
     */
    private static void createIndex(RestHighLevelClient client) throws IOException {

        CreateIndexRequest createRequest = new CreateIndexRequest("java_test");
        //指定索引库的配置信息
        createRequest.settings(Settings.builder().put("index.number_of_shards", 3));
        //执行
        client.indices().create(createRequest, RequestOptions.DEFAULT);
    }
}
