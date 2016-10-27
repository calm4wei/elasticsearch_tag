package com.alfer.es.dao;

import com.alfer.es.common.ClientFactory;
import com.alfer.es.json.JSONObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

import java.util.Random;
import java.util.UUID;

/**
 * Created by feng.wei on 2015/12/3.
 */
public class UseridIndex {

    static String[] citys = {"南京", "北京", "深圳", "上海", "广州", "连云港", "无锡", "大连", "西安", "西宁", "苏州", "杭州"};
    static String[] sexs = {"男", "女"};
    static String[] channels = {"百度", "华为", "小米", "九九畅游", "appstore", "豌豆荚", "360"};
    static String[] numbers = {"1", "2", "3", "4", "5", "6", "7", "8", "9"};
    static String index = "tag";
    static String type = "PeopleProperties";

    public static String generateContent(Random random) {
        String phoneNumber = 1 + "";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("city", citys[random.nextInt(citys.length)]);
        jsonObject.put("sex", sexs[random.nextInt(sexs.length)]);
        jsonObject.put("channel", channels[random.nextInt(channels.length)]);
        for (int i = 0; i < 10; i++) {
            phoneNumber += numbers[random.nextInt(numbers.length)];
        }
        jsonObject.put("phoneNumber", phoneNumber);
        return jsonObject.toString();
    }

    public static JSONObject generateJson(Random random) {
        String phoneNumber = 1 + "";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("city", citys[random.nextInt(citys.length)]);
        jsonObject.put("sex", sexs[random.nextInt(sexs.length)]);
        jsonObject.put("channel", channels[random.nextInt(channels.length)]);
        for (int i = 0; i < 10; i++) {
            phoneNumber += numbers[random.nextInt(numbers.length)];
        }
        jsonObject.put("phoneNumber", phoneNumber);
        return jsonObject;
    }

    public static void insertData(int num, int count, Client client, Random random, Settings settings) {
        String userid;
        boolean flag = true;
//        UpdateRequest updateRequest = new UpdateRequest();

        for (int i = 0; i < num; i++) {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            long t1 = System.currentTimeMillis();
            for (int j = 0; j < count; j++) {
                userid = UUID.randomUUID().toString().replaceAll("-", "");

                // client.admin().indices().prepareCreate("")
                //            .setSettings(settings);
                //            .execute().actionGet();
                // ImmutableSettings.settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", "0")

                // 如果userid不存在就直接插入；如果userid存在则更新
//                flag = client.prepareGet(index, type, userid).get().isExists();
//                if (!flag) {
                    bulkRequestBuilder.add(client.prepareIndex("tag", "PeopleProperties", userid)
                            .setSource(generateContent(random)));

//                } else {
//                    client.prepareUpdate(index, type, userid)
//                            .setDoc(generateContent(random))
//                            .execute();
//                    System.out.println("*************");
//                }
            }
            bulkRequestBuilder.execute();
            long t2 = System.currentTimeMillis();
            System.out.println(count + "条耗时=" + (t2 - t1) + "ms");
            BulkItemResponse[] items = bulkRequestBuilder.get().getItems();
            for (BulkItemResponse item : items) {
                System.out.println("id=" + item.getId() + ", type=" + item.getType() + ", index=" + item.getIndex());
            }
        }
    }

    public static void main(String[] args) {
        Random random = new Random();

        Client client = ClientFactory.getClient();
        // 设置 number_of_replicas, number_of_shards 等索引的属性
        Settings settings = Settings.builder().put("number_of_replicas", 0).build();
        int num = 10;
        int count = 10000;

        long t1 = System.currentTimeMillis();
        insertData(num, count, client, random, settings);
        long t2 = System.currentTimeMillis();
        System.out.println(num * count + "条总耗时=" + ((t2 - t1) / 1000) + " s");
        int totalSec = (int) ((t2 - t1) / 1000);
        System.out.println("平均每秒 " + ((num * count) / totalSec) + " 条数据");

    }
}
