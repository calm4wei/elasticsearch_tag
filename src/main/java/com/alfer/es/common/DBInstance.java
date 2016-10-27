package com.alfer.es.common;

import java.sql.*;

/**
 * Created by feng.wei on 2015/11/20.
 */
public class DBInstance {

    private final static String SQL_DRIVER = "com.ibm.db2.jcc.DB2Driver";
    private String url;
    private String name;
    private String password;
    private Connection connection;

    public DBInstance() {
        try {
            Configuration.conf();
            url = Configuration.prop.getProperty("sql.jdbc.url");
            name = Configuration.prop.getProperty("sql.jdbc.user");
            password = Configuration.prop.getProperty("sql.jdbc.passwd");
            Class.forName(SQL_DRIVER);
            connection = DriverManager.getConnection(url, name, password);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public ResultSet query(String sql, String... args) {
        ResultSet rs = null;
        try {
            PreparedStatement preStat = connection.prepareStatement(sql);
            for (int i = 1; i <= args.length; i++) {
                preStat.setString(i, args[i - 1]);
            }
            rs = preStat.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }


}
