package com.lxs.flink.pgsql.connection;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class PooledConnectionFactory implements Supplier<Connection> {

    private final DruidDataSource dataSource;

    public PooledConnectionFactory(URI uri) {
        this.dataSource = new DruidDataSource();
        final String dbUrl = "jdbc:postgresql://" + uri.getHost() + uri.getPath();
        if (StringUtils.isNotBlank(uri.getUserInfo())) {
            String[] users = uri.getUserInfo().split(":");
            dataSource.setUsername(users[0]);
            dataSource.setPassword(users[1]);
        }
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(dbUrl);
        dataSource.setMaxWait(5);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(5);
    }

    @Override
    public Connection get() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


}
