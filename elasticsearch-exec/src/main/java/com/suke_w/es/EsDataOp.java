package com.suke_w.es;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 针对ES中索引数据的操作
 * 增删改查
 */
public class EsDataOp {
    private static Logger logger = LogManager.getLogger(EsDataOp.class);

    public static void main(String[] args) throws IOException {
        //获取RestClinet连接
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("bigdata01", 9200, "http"),
                new HttpHost("bigdata02", 9200, "http"),
                new HttpHost("bigdata03", 9200, "http")
        ));

        //创建索引（即创建数据）
        //addIndexByJson(client);
        //addIndexByMap(client);
        //查询索引
        //getIndex(client);
        //getIndexByFields(client);
        //更新索引：全量更新和局部更新，可以使用创建索引直接完整更新已存在数据
        //updateByPart(client);
        //删除索引
        //deleteIndex(client);
        //Bulk批量操作
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("emp").id("20").
                source(XContentType.JSON, "fields1", "value1", "fields2", "values2"));
        bulkRequest.add(new DeleteRequest("emp", "10"));  //数据不存在，删除不会报错
        bulkRequest.add(new UpdateRequest("emp", "11")
                .doc(XContentType.JSON, "age", "55"));
        bulkRequest.add(new UpdateRequest("emp", "12")
                .doc(XContentType.JSON, "age", "19"));        //数据不存在，更新会报错
        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemRespons : bulkItemResponses) {
            if (bulkItemRespons.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemRespons.getFailure();
                logger.error("bulk异常：" + failure);
            }
        }
        //关闭连接
        client.close();

    }

    private static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("emp", "10");
        client.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    private static void updateByPart(RestHighLevelClient client) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("emp", "10");
        String jsonString = "{\"age\":22}";
        updateRequest.doc(jsonString, XContentType.JSON);

        client.update(updateRequest, RequestOptions.DEFAULT);
    }

    /**
     * 查询索引部分字段
     *
     * @param client
     * @throws IOException
     */
    private static void getIndexByFields(RestHighLevelClient client) throws IOException {
        GetRequest getRequest = new GetRequest("emp", "10");
        String[] includes = {"name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        //执行
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String id = response.getId();
        String index = response.getIndex();
        if (response.isExists()) { //如果没有查到文档数据，则isExists返回false
            //获取json字符转格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);

            //获取map格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        } else {
            logger.warn("没有查到索引库{}中id为{}的文档", index, id);
        }
    }

    /**
     * 查询索引
     *
     * @param client
     * @throws IOException
     */
    private static void getIndex(RestHighLevelClient client) throws IOException {
        GetRequest getRequest = new GetRequest("emp", "10");
        //执行
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String id = response.getId();
        String index = response.getIndex();
        if (response.isExists()) { //如果没有查到文档数据，则isExists返回false
            //获取json字符转格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);

            //获取map格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        } else {
            logger.warn("没有查到索引库{}中id为{}的文档", index, id);
        }
    }

    private static void addIndexByMap(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("11");
        HashMap<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("name", "zhangsna");
        jsonMap.put("age", 22);
        request.source(jsonMap);
        client.index(request, RequestOptions.DEFAULT);
    }

    private static void addIndexByJson(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("10");
        String json = "{" +
                "\"name\":\"jessic\"," +
                "\"age\":\"32\"" +
                "}";
        request.source(json, XContentType.JSON);
        //执行
        client.index(request, RequestOptions.DEFAULT);
    }
}
