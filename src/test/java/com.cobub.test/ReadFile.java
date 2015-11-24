package com.cobub.test;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;

/**
 * Created by feng.wei on 2015/11/19.
 */
public class ReadFile {

    public static void readTags(){

        try {
            InputStream in = null;
            in = new FileInputStream(new File("tags.properties"));
//            BufferedReader reader = new BufferedReader(new InputStreamReader(in,"UTF-8"));
            Properties prop = new Properties();
            prop.load(in);
//            prop.load(in);
//            String tagsFirst = (String) prop.get("tag.first");
            String tagsFirst = new String(((String) prop.get("tag.first")).getBytes("GBK"),"UTF-8");
            System.out.println("tagsFirst=" + tagsFirst);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        readTags();
    }
}
