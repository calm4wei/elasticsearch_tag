package com.alfer.es.common;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class EsUtils {


    public static Client getClient() {
        return ClientFactory.getClient();
    }

    public static void close(Client client) {
        if (null != client) {
            client.close();
        }
    }

    /**
     * get one's tag information, according to index/type/id
     *
     * @param client
     * @param index
     * @param type
     * @param id
     * @return
     */
    public static String getById(Client client, String index, String type, String id) {
        String result = "";
        GetResponse getResponse = client.prepareGet(index, type, id).get();
        if (getResponse.isExists()) {
            result = getResponse.getSourceAsString();
        }
        // can add external status code
        // JSONObject jsonObject = new JSONObject(result);

        return result;
    }

    /**
     * search tag arrays, according to index/types, as well as field
     *
     * @param client
     * @param index
     * @param types,     types can be a type or multi type that is separate by ",".
     * @param termMap
     * @param operation, and/or
     * @param size
     * @param from
     * @return
     */
    public static SearchHit[] searchByTerm(Client client, String index, String types, Map<String, String> termMap
            , Enum operation, int from, int size) {
        SearchRequestBuilder builder = searchBuilder(client.prepareSearch(), index, types, termMap, operation, from, size);
        SearchResponse searchResponse = builder.get();
        return searchResponse.getHits().getHits();
    }

    /**
     * search userids, according to index/types, as well as field
     *
     * @param client
     * @param index,
     * @param types,     types can be a type or multi type that is separate by ",".
     * @param termMap,
     * @param operation, and/or
     * @param size
     * @param from
     * @return
     */
    public static Set<String> getUids(Client client, String index, String types, Map<String, String> termMap
            , Enum operation, int from, int size) {
        Set<String> set = new HashSet<String>();
        SearchRequestBuilder builder = searchBuilder(client.prepareSearch(), index, types, termMap, operation, from, size);
        // not return sources
        builder.addField("");
        SearchResponse searchResponse = builder.get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            set.add(hit.getId());
        }
        return set;
    }

    /**
     * set response data from, and size that is data count.
     *
     * @param builder
     * @param size
     * @param from
     * @return
     */
    public static SearchRequestBuilder setPage(SearchRequestBuilder builder, int size, int from) {
        return builder.setFrom(from)
                .setSize(size);

    }

    /**
     * build search request by condition
     *
     * @param builder
     * @param index,     can be null
     * @param types,     types can be a type or multi type that is separate by ",".
     * @param termMap,   can be null
     * @param operation, and/or
     * @param size
     * @param from
     * @return
     */
    public static SearchRequestBuilder searchBuilder(SearchRequestBuilder builder, String index, String types
            , Map<String, String> termMap, Enum operation, int from, int size) {

        if (null != index) {
            builder.setIndices(index);
        }
        if (null != types) {
            builder.setTypes(types);
        }
        if (null != termMap && termMap.size() > 0) {
            Iterator<Map.Entry<String, String>> entryIterator = termMap.entrySet().iterator();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (null == operation || operation == EsOperation.GATHER.MUST) {
                while (entryIterator.hasNext()) {
                    Map.Entry<String, String> entry = entryIterator.next();
                    // builder.setQuery(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
                    boolQueryBuilder.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
                }
            } else if (operation == EsOperation.GATHER.SHOULD) {
                while (entryIterator.hasNext()) {
                    Map.Entry<String, String> entry = entryIterator.next();
                    boolQueryBuilder.should(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
                }
            }
            builder.setQuery(boolQueryBuilder);
        }

        builder.setFrom(from)
                .setSize(size);

        return builder;
    }

    /**
     * migrate data from one index and type to another
     *
     * @param client
     * @param srcIndex  index in original datasource
     * @param srcType   type in original datasource
     * @param destIndex index in target datasource
     * @param destType  type in target datasource
     * @return
     */
    public static boolean migrate(Client client, String srcIndex, String srcType, String destIndex, String destType) {
        // scan-scoll
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(srcIndex)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchResponse = searchRequestBuilder
                .execute().actionGet();

        searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int count = 0;
        for (SearchHit hit : searchResponse.getHits()) {
//            System.out.println(hit.getIndex());
//            System.out.println(hit.getType());
//            System.out.println(hit.getSourceAsString());
            bulkRequest.add(client.prepareIndex(destIndex, destType)
                    .setSource(hit.getSourceAsString())
            );

            if (count % 10 == 0) {
                bulkRequest.execute().actionGet();
            }
            count++;
        }


        return false;
    }

    /**
     * function: scan-scroll
     * <br>example</b>
     *
     * @param client
     */
    public void searchIndex(Client client) {

        QueryBuilder qb = QueryBuilders.termQuery("user", "kimchy");
        SearchResponse scrollResp = client.prepareSearch("twitter")
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
        //Scroll until no hits are returned
        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            for (SearchHit hit : scrollResp.getHits()) {
                Iterator<Map.Entry<String, Object>> rpItor = hit.getSource().entrySet().iterator();
                while (rpItor.hasNext()) {
                    Map.Entry<String, Object> rpEnt = rpItor.next();
                    System.out.println(rpEnt.getKey() + " : " + rpEnt.getValue());
                }
            }
            //Break condition: No hits are returned
            if (scrollResp.getHits().hits().length == 0) {
                break;
            }
        }
    }

}
