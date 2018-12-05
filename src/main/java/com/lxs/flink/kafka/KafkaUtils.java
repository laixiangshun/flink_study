package com.lxs.flink.kafka;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka 工具类
 **/
public class KafkaUtils {
    private static final String broker_list = "192.168.20.48:9092";
    private static final String topic = "metric";
//    private static final String zookeeper = "192.168.20.48:9092168.20.48:2181,192.168.20.51.2181,192.168.20.52:2181";
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        while (true) {
            try {
                writeToKafka();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void writeToKafka() throws JsonProcessingException {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker_list);
//        prop.put("zookeeper.connect", zookeeper);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, mapper.writeValueAsString(metric));
        producer.send(record);
        System.out.println("发送数据：" + mapper.writeValueAsString(metric));
        producer.flush();
    }
}
