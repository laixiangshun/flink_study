package com.lxs.flink.kafka;

import com.lxs.flink.model.Student;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/5
 **/
public class KafkaUtils2 {

    private static final String broker_list = "192.168.20.48:9092";
    private static final String topic = "metric";
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            writeDataToKafka();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private static void writeDataToKafka() throws JsonProcessingException {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker_list);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "name" + i, "password" + i, 5 + i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, mapper.writeValueAsString(student));
            producer.send(record);
            System.out.println("发送数据：" + mapper.writeValueAsString(student));
        }
        producer.flush();
    }
}
