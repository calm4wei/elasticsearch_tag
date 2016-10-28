package com.alfer.es.earth;

import com.alfer.es.common.ClientFactory;
import com.alfer.es.common.EsUtils;
import com.alfer.es.json.JSONObject;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2016/10/8
 *
 * @author feng.wei
 */
public class EarthData {

    public static void main(String[] args) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("author", "李金");
//        params.put("kw_cn", "波形");
        SearchHit[] hits = EsUtils.searchByTerm(ClientFactory.getClient(), "my_index_v2", "dz", params, null, 1, 1000);
        for (SearchHit hit : hits) {
            System.out.println("==============================");
            JSONObject jsonObject = new JSONObject(hit.getSourceAsString());
            System.out.println("score=" + hit.getScore() + ", title_cn=" + jsonObject.getString("title_cn") + "kw_cn=" + jsonObject.get("kw_cn"));
        }
        System.out.println(hits.length);
    }
}
