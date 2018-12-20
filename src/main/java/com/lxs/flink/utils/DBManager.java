package com.lxs.flink.utils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * 数据库工具类
 * Created by zsq on 2018/8/31.
 */
public class DBManager {
    private static Connection connection;
    private static QueryRunner queryRunner = new QueryRunner();

    public static Connection connection() {
        if (connection == null) {
            synchronized (DBManager.class) {
                try {
                    Class.forName("com.mysql.jdbc.Driver");
//                    connection = DriverManager.getConnection(Constants.prop.getProperty("jdbc.url"), Constants.prop.getProperty("jdbc.user"), Constants.prop.getProperty("jdbc.pass"));
                    connection = JdbcUtil.getJdbcTemplate().getDataSource().getConnection();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return connection;
    }

    public static boolean create(String sql) {
        Statement stmt = null;
        try {
            stmt = connection().createStatement();
            stmt.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean drop(String sql) {
        Statement stmt = null;
        try {
            stmt = connection().createStatement();
            stmt.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Long insert(String sql, Object... params) {
        try {
            return queryRunner.insert(connection(), sql, new ScalarHandler<>(), params);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0l;
        }
    }

    public static Integer update(String sql, Object... params) {
        try {
            return queryRunner.update(connection(), sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static <T> T query(String sql, ResultSetHandler<T> rsh, Object... params) {
        try {
            return queryRunner.query(connection(), sql, rsh, params);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
