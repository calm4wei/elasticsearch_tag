package com.cobub.test;

import com.alfer.es.common.ClientFactory;
import com.alfer.es.common.EsOperation;
import com.alfer.es.json.JSONObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class SearchIndex {

    Client client2 = null;

    @Before
    public void init() {
        client2 = ClientFactory.getClient();
//        client2 = ClientFactory.getClientByNode();
    }

    @After
    public void close() {
        if (null != client2) {
            client2.close();
        }
    }

    @Test
    public void search() {
        SearchResponse response = client2.prepareSearch("index1", "index2")
                .setTypes("type1", "type2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        // MatchAll on the whole cluster with all default options
//        SearchResponse response = client.prepareSearch().execute().actionGet();
    }

    @Test
    public void queryTest() throws IOException {
        try {
            BufferedReader bodyReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\common.template"), "utf8"));
            String line = null;
            StringBuilder strBuffer = new StringBuilder();
            while ((line = bodyReader.readLine()) != null) {
                strBuffer.append(line);
                strBuffer.append("\n");
            }

            Map<String, Object> search_params = new HashMap();
            search_params.put("from", 1);
            search_params.put("size", 5);
            search_params.put("key_words", "opencv sift");

            QueryBuilder qb = QueryBuilders.templateQuery(strBuffer.toString(), search_params);
            SearchResponse sr;
            SearchRequestBuilder srb;
            srb = client2.prepareSearch("twitter")
                    .setTypes("tweet")
                    .setQuery(qb);
            sr = srb.get();

            for (SearchHit hit : sr.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        } catch (UnsupportedEncodingException ex) {

        } catch (FileNotFoundException ex) {

        } catch (IOException ex) {

        }

    }

    @Test
    public void matchAllQueryTest() {
        long t1 = System.currentTimeMillis();
        SearchResponse response = client2.prepareSearch()
//                .addFields("user")
                .setQuery(QueryBuilders.matchAllQuery())
                        //.setFrom(0)
                .setSize(20000000)
                .execute().actionGet();
        long t2 = System.currentTimeMillis();
//        System.out.println("total=" + response.getTotalShards());
        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        System.out.println("lenght=" + hits.getHits().length);
        System.out.println("t2-t1=" + (t2 - t1));
//         for (SearchHit sh : hit) {
//             System.out.println(sh.getSourceAsString() + ", " + sh.getIndex());
//         }

    }

    @Test
    public void filterQueryTest() {
        long t1 = System.currentTimeMillis();
        SearchResponse response = client2.prepareSearch()
                .setIndices("customer")
                .setTypes("ik")
//                .setQuery(QueryBuilders.matchQuery("user", "3342"))
                .setQuery(QueryBuilders.termQuery("用户", "小"))
//                .addField("user")
//                .setQuery(QueryBuilders.regexpQuery("user", "99*"))
                .setFrom(0)
                .setSize(10000000)
                .execute().actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        long t2 = System.currentTimeMillis();
        System.out.println("t2-t1=" + ((t2 - t1) / 1000) + " s");
        System.out.println("length=" + hit.length);
        for (SearchHit sh : hit) {
            System.out.println(sh.getSourceAsString() + ", index=" + sh.getIndex() + ", type=" + sh.getType());
        }

    }

    @Test
    public void QueryByIndexAndType() throws UnsupportedEncodingException {
        long t1 = System.currentTimeMillis();
        SearchResponse response = client2.prepareSearch()
                .setQuery(QueryBuilders.typeQuery("book"))
//                .setQuery(QueryBuilders.typeQuery("spark"))
//                .setQuery(QueryBuilders.typeQuery("hadoop"))
//                .setQuery(QueryBuilders.idsQuery("book"))
                .setIndices("razor", "cobub")
                .setTypes("book", "spark", "hadoop")
//                .setQuery(QueryBuilders.regexpQuery("user", "[0-9A-Za-z]*99[0-9A-Za-z]*"))
//                .setQuery(QueryBuilders.regexpQuery("message", "[0-9A-Za-z]*88[0-9A-Za-z]*"))
//                .setQuery(QueryBuilders.regexpQuery("message", "99*"))
//                .setQuery(QueryBuilders.boolQuery()
//                                .must(QueryBuilders.regexpQuery("user", "[0-9A-Za-z]*99[0-9A-Za-z]*"))
////                                .must(QueryBuilders.regexpQuery("user", "[0-9A-Za-z]*88[0-9A-Za-z]*"))
//                                .must(QueryBuilders.regexpQuery("message", "[0-9A-Za-z]*888[0-9A-Za-z]*"))
//
//                )
//                .setQuery(QueryBuilders.boolQuery()
//                                .should(QueryBuilders.regexpQuery("user", "00*"))
//                                .must(QueryBuilders.regexpQuery("message", "[0-9A-Za-z]*888[0-9A-Za-z]*"))
//                )

                .setFrom(0)
                .setSize(10000000)
                .execute().actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        long t2 = System.currentTimeMillis();
        for (SearchHit sh : hit) {
            System.out.println(sh.getSourceAsString() + ", index=" + sh.getIndex() + ", type=" + sh.getType());
        }
        System.out.println("t2-t1=" + (t2 - t1) + " ms");
        System.out.println("t2-t1=" + ((t2 - t1) / 1000) + " s");
        System.out.println("length=" + hit.length);

    }


    /**
     * search Nested datatype
     * example:
     * GET my_index/_search
     * {
     * "query": {
     * "bool": {
     * "must": [
     * { "match": { "user.first": "Alice" }},
     * { "match": { "user.last":  "White" }}
     * ]
     * }
     * }
     * }
     * <p/>
     * curl -XGET 'http://localhost:9200/1/1/_search?pretty=true' -d '{"query":{"bool":{"must":[{"match":{"type":"性别"}},{"match":{"tags":"英语"}},{"match":{"tags":"自行车"}}]}}}'
     * <p/>
     * curl -XGET 'http://localhost:9200/1/1/_search?pretty=true' -d '{"query":{"bool":{"must":{"match":{"type":"性别"}}}}}'
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void tagQueryTest() throws UnsupportedEncodingException {
        long t1 = System.currentTimeMillis();
//        String s = new String("人口属性".getBytes(),"UTF-8");
        SearchResponse response = client2.prepareSearch()
//                .setQuery(QueryBuilders.regexpQuery("index", "([0-9a-zA-Z]*[\\u4E00-\\u9FA5]*[0-9a-zA-Z]*)"))
                .setIndices("1", "6")
                .setTypes("2", "35", "36")
//                .setQuery(QueryBuilders.termQuery("type", "性别"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("tags", "爱骑自行车"))
                        .must(QueryBuilders.matchQuery("tags", "消费水平极高")))
                .setFrom(0)
                .setSize(10000000)
                .execute().actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        long t2 = System.currentTimeMillis();
        for (SearchHit sh : hit) {
            System.out.println(sh.getSourceAsString() + ", index=" + sh.getIndex() + ", type=" + sh.getType() + "ID=" + sh.getId());
        }
        System.out.println("t2-t1=" + ((t2 - t1) / 1000) + " s");
        System.out.println("length=" + hit.length);

    }


    @Test
    public void userIdQueryTest() throws UnsupportedEncodingException {
        long t1 = System.currentTimeMillis();
//        String s = new String("人口属性".getBytes(),"UTF-8");
        SearchResponse response = client2.prepareSearch()
//                .setQuery(QueryBuilders.typeQuery("test"))
//                .setQuery(QueryBuilders.typeQuery("test2"))
                .setIndices("tag")
//                .setTypes("PeopleProperties")
//                .setQuery(QueryBuilders.termQuery("phoneNumber", "13127413683"))
//                .setQuery(QueryBuilders.termQuery("city", "南京"))
//                .setQuery(QueryBuilders.termQuery("channel", "华为"))
//                .setQuery(QueryBuilders.regexpQuery("user", "[0-9A-Za-z]*99[0-9A-Za-z]*"))
//                .setQuery(QueryBuilders.boolQuery()
//                                .must(QueryBuilders.matchQuery("city", "南京"))
//                                .must(QueryBuilders.matchQuery("channel", "华为"))
//                )
//                        .must(QueryBuilders.matchQuery("sex", "男"))
//                        .must(QueryBuilders.matchQuery("channel", "小米"))
//                        .must(QueryBuilders.regexpQuery("phoneNumber", "131[0-9]*")))
                .setFrom(0)
                .setSize(1000000)
//                .addField("")
                .get();
//                .execute().actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        long t2 = System.currentTimeMillis();
        for (SearchHit sh : hit) {
            System.out.println(sh.getSourceAsString() + ", index=" + sh.getIndex() + ", type=" + sh.getType() + ", id=" + sh.getId());
        }
        System.out.println("t2-t1=" + ((t2 - t1) / 1000) + " s");
        System.out.println("length=" + hit.length);

    }

    @Test
    public void getUser() {
        GetResponse getResponse =
                client2.prepareGet("tag", "PeopleProperties", "62537fb28266471eba334bb99f6e502a").get();

        Map<String, Object> map = getResponse.getSource();

        Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }

        System.out.printf("==========");
        GetField getField = getResponse.getField("city");
        System.out.println(getResponse.isExists() + " " + getResponse.isSourceEmpty());

        System.out.println("getSourceAsString=" + getResponse.getSourceAsString());
        JSONObject jsonObject = new JSONObject(getResponse.getSourceAsString());
        jsonObject.put("code", 111);
        System.out.println(jsonObject.toString());
    }

    @Test
    public void testEnum() {
        System.out.println(EsOperation.GATHER.MUST == EsOperation.GATHER.MUST);
        System.out.println(EsOperation.GATHER.MUST.equals(EsOperation.GATHER.MUST));
        Enum anEnum = EsOperation.GATHER.MUST;
        System.out.println(anEnum == null);
        System.out.println(anEnum == EsOperation.GATHER.MUST);

    }

    @Test
    public void test_cardinality() {
        AggregationBuilders.terms("useridentifier").field("useridentifier");
    }

}
