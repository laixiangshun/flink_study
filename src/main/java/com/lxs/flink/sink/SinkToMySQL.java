package com.lxs.flink.sink;

import com.lxs.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/5
 **/
public class SinkToMySQL extends RichSinkFunction<Student> {

    private Connection connection;

    private PreparedStatement pre;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student(name,age,password) values(?,?,?);";
        pre = connection.prepareStatement(sql);
    }

    private Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_source?userSSL=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT", "root", "root");
        } catch (Exception e) {
            System.err.println("connection to mysql has exception,msg=" + e.getMessage());
        }
        return con;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (pre != null) {
            pre.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     **/
    @Override
    public void invoke(Student value, Context context) throws Exception {
        pre.setString(1, value.getName());
        pre.setInt(2, value.getAge());
        pre.setString(3, value.getPassword());
        pre.executeUpdate();
    }
}
