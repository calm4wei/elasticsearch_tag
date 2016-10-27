package com.alfer.es.earth;

import com.alfer.es.common.ClientFactory;
import com.alfer.es.common.EsUtils;
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
        Map<String,String> params = new HashMap<String, String>();
        params.put("title_cn", "地震破裂");
        params.put("title_cn", "波形");
        SearchHit[] hits = EsUtils.searchByTerm(ClientFactory.getClient(), "taiwang", "dz", null, null, 1, 1000);
        for (SearchHit hit : hits) {
            System.out.println("==============================");
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(hits.length);
    }
}
