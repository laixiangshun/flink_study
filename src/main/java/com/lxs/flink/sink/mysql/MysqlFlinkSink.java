package com.lxs.flink.sink.mysql;

import com.lxs.flink.model.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 从kafka source读取数据，并使用自定义的sink将数据保存到mysql
 **/
public class MysqlFlinkSink {
    private static final String broker_list = "192.168.20.48:9092";
    private static final String quorum = "192.168.20.48:2181,192.168.20.52:2181,192.168.20.51:2181";
    private static final String topic = "metric";
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker_list);
        prop.put("zookeeper.connect", quorum);
        prop.put("group.id", "student-group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<Student> operator = streamSource.setParallelism(1).map(s -> mapper.readValue(s, Student.class));
//        operator.print();//等同于
//        operator.addSink(new PrintSinkFunction<>());

        //使用自定义的sink 将数据保存到mysql
        operator.addSink(new SinkToMySQL());
        try {
            env.execute("flink add sink mysql");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
