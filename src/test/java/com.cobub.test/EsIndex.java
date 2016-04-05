package com.cobub.test;

import com.cobub.es.common.ClientFactory;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class EsIndex {

    Client client = null;
//    String json = "{" +
//            "\"user\":\"kimchy\"," +
//            "\"postDate\":\"2013-01-30\"," +
//            "\"message\":\"trying out Elasticsearch\"" +
//            "}";

    String[] indexs = {"razor", "cobub", "twitter", "google", "baidu", "facebook", "alibaba"};
    String[] tags = {"book", "spark", "hadoop", "hdfs", "es", "flume", "hbase", "mongodb", "linux", "java", "scala", "python",
            "English", "China", "nanjing", "beijing", "suzhou", "shanghai", "woainizhongguo"};

//    String[] indexs = {"razor", "cobub"};
//    String[] tags = {"英语","汉语"};

    @Before
    public void init() {
        client = ClientFactory.getClient();
    }

    @After
    public void close() {
        client.close();
    }

    @Test
    public void generateData() throws UnsupportedEncodingException {
        final Random random = new Random();
        random.nextInt(indexs.length - 1);

        long t1 = System.currentTimeMillis();
        for (int k = 0; k < 1; k++) {
            postIndexData(indexs[random.nextInt(indexs.length - 1)], tags[random.nextInt(tags.length - 1)], 1);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("t2 - t1=" + ((t2 - t1) / 1000) + " ms");

    }


    private void postIndexData(String index, String type, Integer num) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 0; i < num; i++) {
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            String name = uuid.substring(0, 4);
            String dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

            String json = "{" +
                    "\"user\":\"" + name + "\"," +
                    "\"postTime\":\"" + dateFormat + "\"," +
                    "\"message\":\"" + uuid + "\"" +
                    "}";


            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(json));
//            IndexResponse response = client.prepareIndex(index, type)
//                    .setSource(json)
//                    .get();
//            System.out.println("isCrete=" + response.isCreated());
            // Index name
//            String _index = response.getIndex();
//            // Type name
//            String _type = response.getType();
//            // Document ID (generated or not)
//            String _id = response.getId();
//            // Version (if it's the first time you index this document, you will get: 1)
//            long _version = response.getVersion();
//            // isCreated() is true if the document is a new one, false if it has been updated
//            boolean created = response.isCreated();
//            System.out.println("index=" + _index + ",type=" + _type + ",id=" + _id + ",created=" + created + ",i=" + i);
        }
        BulkItemResponse[] items = bulkRequest.get().getItems();
        for (BulkItemResponse item : items) {
            System.out.println("item=" + item.getIndex());
        }
    }

    @Test
    public void indexMapperData() throws UnsupportedEncodingException {

        Map<String, Object> map = new HashMap<String, Object>();
        String name = new String("杰".getBytes(), "UTF-8");
        map.put("user", name);
        map.put("postDate", new Date());
        map.put("message", "中文测试2");
        // index=1,type=1,id=AVE0DfnKWryyMC7C6QIA
        IndexResponse response = client.prepareIndex("index", "fulltext")
                .setId("_mapping")
                .setSource(map)
                .get();

        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();

        System.out.println("index=" + _index + ",type=" + _type + ",id=" + _id + ",version=" + _version + ",created=" + created);
    }


    @Test
    public void getData() {
//        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
        GetResponse response = client.prepareGet("twitter", "tweet", "1")
                .setOperationThreaded(false)
                .get();
        client.prepareSearch("index_app").setSearchType(SearchType.SCAN).setScroll(
                new TimeValue(100000)).setQuery(QueryBuilders.matchQuery("","")).execute();
//        GetResponse response1 = client.prepareGet().get();
//        Iterator<Map.Entry<String,GetField>> iter = response1.getFields().entrySet().iterator();
//
//        while (iter.hasNext()){
//            Map.Entry entry = iter.next();
//            System.out.println("key=" + entry.getKey() + ",value=" + entry.getValue());
//        }


        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();

        System.out.println("index=" + _index + ",type=" + _type + ",id=" + _id + ",version=" + _version);

    }


    @Test
    public void delData() {
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
                .get();
        System.out.println("id=" + response.getId() + ",isFound=" + response.isFound());
    }


    @Test
    public void multiGet() {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("twitter", "tweet", "AVD_iqpd5Dy3LoQ0CUCY")
//                .add("twitter", "tweet", "2", "3", "4")
//                .add("another", "type", "foo")
                .get();

//        GetRequest request = new GetRequest("twitter");
//        MultiGetRequest multiGetRequest = new MultiGetRequest();
//        ActionFuture<MultiGetResponse> actionFuture = client.multiGet(multiGetRequest);
//        MultiGetItemResponse[] responses = actionFuture.actionGet().getResponses();
//        for (MultiGetItemResponse itemResponse : responses){
//            GetResponse response = itemResponse.getResponse();
//            if (response.isExists()) {
//                String json = response.getSourceAsString();
//                System.out.println("json=" + json);
//            }
//        }
//        System.out.println("==========================");

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println("json=" + json);
            }
        }
    }

    @Test
    public void bulkApi() throws IOException {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

// either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "trying out Elasticsearch")
                                        .endObject()
                        )
        );

//        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
//                        .setSource(jsonBuilder()
//                                        .startObject()
//                                        .field("user", "kimchy")
//                                        .field("postDate", new Date())
//                                        .field("message", "another post")
//                                        .endObject()
//                        )
//        );

        BulkResponse bulkResponse = bulkRequest.get();
        BulkItemResponse[] items = bulkResponse.getItems();

        for (BulkItemResponse item : items) {
            System.out.println("item=" + item.getIndex());
        }

//        if (bulkResponse.hasFailures()) {
//            // process failures by iterating through each bulk response item
//        }
    }

    @Test
    public void updateData() throws IOException, ExecutionException, InterruptedException {
//        UpdateRequest updateRequest = new UpdateRequest();
//        updateRequest.index("index");
//        updateRequest.type("type");
//        updateRequest.id("1");
//        updateRequest.doc(jsonBuilder()
//                .startObject()
//                .field("gender", "male")
//                .endObject());
//        client.update(updateRequest).get();
//
//        client.prepareUpdate("ttl", "doc", "1")
//                .setScript(new Script("ctx._source.gender = \"male\"", ScriptService.ScriptType.INLINE, null, null))
//                .get();

        System.out.println(client.prepareGet("tag","PeopleProperties","2bbeaa35d4794cf8ab85e1259ee30af9").get().isExists());
        UpdateResponse response = client.prepareUpdate("tag", "PeopleProperties", "2bbeaa35d4794cf8ab85e1259ee30af9")
                .setDoc("city","贵州")
                .execute()
                .get();

        System.out.println(response.getId() + " " + response.getIndex() + " " + response.getType());

//        System.out.println(fileds.getName() + " " + fileds.getValue().toString());
    }
//    public static void main(String[] args) {
//        new EsIndex().generateData();
//    }

}
