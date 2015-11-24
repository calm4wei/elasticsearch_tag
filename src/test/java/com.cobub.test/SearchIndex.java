package com.cobub.test;

import com.cobub.es.common.ClientFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class SearchIndex {

    Client client2 = null;

    @Before
    public void init() {
        client2 = ClientFactory.getClient();

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
//        Node node = NodeBuilder.nodeBuilder()
//                .clusterName("my-application")
//                .client(true).node();
////        ����elastic�ͻ���
//        Client client = node.client();

        //��ȡ��ѯģ�壬Ȼ�����ò�����ѯ
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
        SearchResponse response = client2.prepareSearch()
//                .addFields("user")
                .setQuery(QueryBuilders.matchAllQuery())
                        //.setFrom(0)
                .setSize(1000000)
                .execute().actionGet();
//        System.out.println("total=" + response.getTotalShards());
        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        System.out.println("lenght=" + hits.getHits().length);
        // for (SearchHit sh : hit) {
        //     System.out.println(sh.getSourceAsString() + ", " + sh.getIndex());
        // }

    }

    @Test
    public void filterQueryTest() {
        long t1 = System.currentTimeMillis();
        SearchResponse response = client2.prepareSearch()
                .setQuery(QueryBuilders.typeQuery("java"))
                .setQuery(QueryBuilders.typeQuery("book"))
                .setIndices("razor", "cobub")
//                .setQuery(QueryBuilders.matchQuery("user", "3342"))
//                .setQuery(QueryBuilders.termQuery("user", "tom"))
//                .addField("user")
                .setQuery(QueryBuilders.regexpQuery("user", "99*"))
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


    @Test
    public void tagQueryTest() throws UnsupportedEncodingException {
        long t1 = System.currentTimeMillis();
//        String s = new String("人口属性".getBytes(),"UTF-8");
        SearchResponse response = client2.prepareSearch()
//                .setIndices("1", "6")
//                .setTypes("2")
//                .setQuery(QueryBuilders.regexpQuery("userid", "99*"))
//                .setQuery(QueryBuilders.regexpQuery("index", "([0-9a-zA-Z]*[\\u4E00-\\u9FA5]*[0-9a-zA-Z]*)"))
                .setIndices("1", "2")
//                .setQuery(QueryBuilders.termQuery("type", "性别"))
                .setQuery(QueryBuilders.boolQuery()
//                                .must(QueryBuilders.matchQuery("type", "性别"))
                        .must(QueryBuilders.matchQuery("tags", "英语"))
                        .must(QueryBuilders.matchQuery("tags", "消费")))

                .setFrom(0)
                .setSize(1000)
                .execute().actionGet();

        SearchHits hits = response.getHits();
        SearchHit[] hit = hits.getHits();
        long t2 = System.currentTimeMillis();
        System.out.println("t2-t1=" + ((t2 - t1) / 1000) + " s");
        System.out.println("length=" + hit.length);
        for (SearchHit sh : hit) {
            System.out.println(sh.getSourceAsString() + ", index=" + sh.getIndex() + ", type=" + sh.getType() + "ID=" + sh.getId());
        }

    }
}
