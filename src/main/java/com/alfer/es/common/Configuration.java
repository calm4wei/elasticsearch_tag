package com.alfer.es.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class Configuration {

    public static Properties prop = new Properties();

    public static void conf() {
        try {
            InputStream in = Configuration.class.getClassLoader().getResourceAsStream("db.properties");
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
