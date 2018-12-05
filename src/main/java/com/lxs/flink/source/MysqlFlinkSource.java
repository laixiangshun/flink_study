package com.lxs.flink.source;

import com.lxs.flink.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/5
 **/
public class MysqlFlinkSource {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> streamSource = env.addSource(new SourceFromMySQL()).setParallelism(1);
        streamSource.print();
        try {
            env.execute("add mysql source");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
