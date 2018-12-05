package com.lxs.flink.source;

import com.lxs.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

/**
 * 从mysql 读取数据的source
 **/
public class SourceFromMySQL extends RichSourceFunction<Student> {
    private PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     **/
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from Student";
        ps = connection.prepareStatement(sql);
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_source?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        } catch (Exception e) {
            System.err.println("connection mysql has exception,msg=" + e.getMessage());
        }
        return con;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     **/
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student();
            student.setId(resultSet.getInt("id"));
            student.setAge(resultSet.getInt("age"));
            student.setName(resultSet.getNString("name"));
            student.setPassword(resultSet.getString("password"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {

    }
}
