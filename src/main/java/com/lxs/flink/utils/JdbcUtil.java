package com.lxs.flink.utils;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @author tangqiang
 * @date 2018/09/21
 */
public class JdbcUtil {

    private static JdbcTemplate jdbcTemplate;

    static {
        DataSource dataSource = DataSourceBuilder.create()
                .driverClassName("com.mysql.jdbc.Driver")
                .url("jdbc:mysql://192.168.10.123:3306/beijing_congestion?useUnicode=true&characterEncoding=utf-8&useSSL=false")
                .username("root")
                .password("Bigdata@123")
                .type(DruidDataSource.class).build();

        jdbcTemplate = new JdbcTemplate(dataSource);
    }


    public static JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
}
