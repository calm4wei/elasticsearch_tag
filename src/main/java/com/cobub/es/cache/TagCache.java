package com.cobub.es.cache;

import com.cobub.es.common.DBInstance;
import com.cobub.es.domain.IndexBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class TagCache {

    private static Map<String, String> indexCache = new HashMap<String, String>();
    private static Map<String, Map<String, String>> typesCache = new HashMap<String, Map<String, String>>();

    public static Map<String, String> getIndexCache() {
        return indexCache;
    }

    public static Map<String, Map<String, String>> getTypesCache() {
        return typesCache;
    }

    static {
        try {
            DBInstance db = new DBInstance();
            String sql = "select id, name from TAG_INDEX";
            ResultSet rs = db.query(sql);
            while (rs.next()) {
                Map <String, String> map = new HashMap<String, String>();
                String id = rs.getString("id");
                String name = rs.getString("name");
                indexCache.put(name,id);
                String sql2 = "select id, name from TAG_TYPES where pid = ?";
                ResultSet types = null;
                types = db.query(sql2,id);
                while (types.next()) {
                    map.put(types.getString("name"),types.getString("id"));
                }
                typesCache.put(name,map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        for (String key : indexCache.keySet()){
            System.out.print("name=" + key + ", id=" + indexCache.get(key));
            System.out.println("    ==     ");

            Map<String,String> map = typesCache.get(key);
            for (String key2 : map.keySet()){
                System.out.println("    " + "name=" + key2 + ", id=" + map.get(key2));
            }
        }
    }

}
