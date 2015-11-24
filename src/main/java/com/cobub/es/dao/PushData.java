package com.cobub.es.dao;

import com.cobub.es.cache.TagCache;
import com.cobub.es.common.ClientFactory;
import com.cobub.es.domain.IndexBean;
import com.cobub.es.domain.TypeBean;
import com.cobub.es.json.JSONArray;
import com.cobub.es.json.JSONObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class PushData {

    private static Logger logger = LoggerFactory.getLogger(PushData.class);

    private static List<IndexBean> indexBeanList = null;
    public static String[] channels = {"百度", "360", "豌豆荚", "小米", "华为", "九九应用"};
    public static String[] tags = {"汉语", "英语", "巴基斯坦", "消费水平极高", "喜欢踢足球", "爱骑自行车", "独立", "游泳", "25"};

    public static List<IndexBean> mapConvertList() {
        indexBeanList = new ArrayList<IndexBean>();
        Map<String, String> indexMap = TagCache.getIndexCache();
        Map<String, Map<String, String>> typeMap = TagCache.getTypesCache();
        for (String key : indexMap.keySet()) {
            IndexBean indexBean = new IndexBean(indexMap.get(key), key);
            List<TypeBean> beanList = new ArrayList<TypeBean>();
            Map<String, String> map = typeMap.get(key);
            for (String key2 : map.keySet()) {
                TypeBean typeBean = new TypeBean(map.get(key2), key2);
                beanList.add(typeBean);
            }
            indexBean.setTypeBeans(beanList);
            indexBeanList.add(indexBean);
        }
        return indexBeanList;
    }

    public static String generateJson(String index, String type) {

        String userid = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 15);
        Random random = new Random();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("userid", userid);
        jsonObject.put("index", index);
        jsonObject.put("type", type);
        jsonObject.put("version", random.nextInt(10));
        jsonObject.put("channel", channels[random.nextInt(channels.length)]);
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < random.nextInt(tags.length); i++) {
            jsonArray.put(tags[random.nextInt(tags.length)]);
        }
        jsonObject.put("tags", jsonArray);
        return jsonObject.toString();
    }

    public IndexBean random() {
        Random random = new Random();
        return indexBeanList.get(random.nextInt(indexBeanList.size()));
    }

    public static void postIndexData(BulkRequestBuilder bulkRequest, Client client, int num) {
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Random random = new Random();
        long t1 = System.currentTimeMillis();
        long t2;
        long t3;
        for (int i = 1; i < num; i++) {

            IndexBean indexBean = indexBeanList.get(random.nextInt(indexBeanList.size()));
            String indexId = indexBean.getId();
            String indexName = indexBean.getName();
            String typeId = indexBean.getTypeBeans().get(random.nextInt(indexBean.getTypeBeans().size())).getId();
            String typeName = indexBean.getTypeBeans().get(random.nextInt(indexBean.getTypeBeans().size())).getName();

            bulkRequest.add(client.prepareIndex(indexId, typeId)
                    .setSource(PushData.generateJson(indexName, typeName)));

            t3 = System.currentTimeMillis();
            if (t3 - t1 >= 1000){
                System.out.println("t3-t1=" + (t3-t1));
            }

        }
        BulkItemResponse[] items = bulkRequest.get().getItems();
        for (BulkItemResponse item : items) {
            System.out.println("===index=" + item.getIndex() + ",type=" + item.getType() + ", id=" + item.getId());
        }
        t2 = System.currentTimeMillis();
        System.out.println(num + "条耗时=" + ((t2 - t1)) + " ms");
    }


    public static void postIndexData(Client client, int num) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Random random = new Random();
        long t1 = System.currentTimeMillis();
        long t2;
        long t3;
        for (int i = 1; i < num; i++) {

            IndexBean indexBean = indexBeanList.get(random.nextInt(indexBeanList.size()));
            String indexId = indexBean.getId();
            String indexName = indexBean.getName();
            int typeRandom = random.nextInt(indexBean.getTypeBeans().size());
            String typeId = indexBean.getTypeBeans().get(typeRandom).getId();
            String typeName = indexBean.getTypeBeans().get(typeRandom).getName();

            bulkRequest.add(client.prepareIndex(indexId, typeId)
                    .setSource(PushData.generateJson(indexName, typeName)));

            t3 = System.currentTimeMillis();
            if (t3 - t1 >= 1000){
                System.out.println("t3-t1=" + (t3-t1));
            }

        }
        BulkItemResponse[] items = bulkRequest.get().getItems();
        for (BulkItemResponse item : items) {
            System.out.println("===index=" + item.getIndex() + ",type=" + item.getType() + ", id=" + item.getId());
        }
        t2 = System.currentTimeMillis();
        System.out.println(num + "条耗时=" + ((t2 - t1)) + " ms");
    }

    public static void main(String[] args) {
        Client client = ClientFactory.getClient();

        PushData.mapConvertList();
        long t1 = System.currentTimeMillis();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        int num = 10;
        int count = 1;
        long t11 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
//            postIndexData(bulkRequest,client,num);
            postIndexData(client,num);
        }
        long t22 = System.currentTimeMillis();

        System.out.println(num * count + "条，耗时=" + (t22 - t11) + " ms");
        int totalSec = (int) ((t22 - t11) / 1000);
        System.out.println("平均每秒 " + ((num * count) / totalSec) + " 条数据");
//        BulkItemResponse[] items = bulkRequest.get().getItems();
//        for (BulkItemResponse item : items) {
//            System.out.println("index=" + item.getIndex() + ",type=" + item.getType() + ", id=" + item.getId());
//        }
    }
}
