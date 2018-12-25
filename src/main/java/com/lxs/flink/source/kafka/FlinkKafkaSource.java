package com.lxs.flink.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * kafka source
 **/
public class FlinkKafkaSource {

    private static final String broker = "192.168.20.48:9092";
    private static final String topic = "metric";
    private static final String zookeeper = "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("zookeeper.connect", zookeeper);
        prop.put("group.id", "metric-group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop)).setParallelism(1);
        dataStreamSource.print();
        try {
            env.execute("Flink add kafka DataSource");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
