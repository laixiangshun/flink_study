package com.lxs.flink.producer;

import com.lxs.flink.utils.MemoryUsageExtrator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 发送监控服务内存数据信息
 **/
public class MemoryKafkaProducer {
    public static void main(String[] args) {
        while (true) {
            try {
                sendMessage();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendMessage() throws UnknownHostException {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.20.48:9092");
        prop.put("acks", "all");
        prop.put("retries", 0);
        prop.put("batch.size", 16318);
        prop.put("linger.ms", 1);
        prop.put("buffer.memory", 33554432);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        InetAddress localHost = InetAddress.getLocalHost();
        String hostName = localHost.getHostName();

        String message = String.format("%d,%s,%d", System.currentTimeMillis(), hostName, MemoryUsageExtrator.currentFreeMemorySizeInBytes());
        ProducerRecord<String, String> record = new ProducerRecord<>("memory", message);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("failed to send kafka data with message:" + e);
            }
        });
        producer.flush();
        producer.close();
    }
}
