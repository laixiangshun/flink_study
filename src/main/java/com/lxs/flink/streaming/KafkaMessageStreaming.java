package com.lxs.flink.streaming;

import com.lxs.flink.watermark.MessageWaterEmitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Properties;

/**
 * flink 处理 kafka中监控服务内存信息数据
 *
 * @author lxs
 **/
public class KafkaMessageStreaming {
    private static final String outPath = "file:///C:/Users/hasee/Desktop/result.txt";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置启动检查点
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.20.48:9092");
        prop.put("zookeeper.connect", "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        prop.put("group.id", "memory");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("memory", new SimpleStringSchema(), prop);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        SingleOutputStreamOperator<Tuple2<String, Long>> dataStream = env.addSource(consumer)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if (s != null && s.contains(",")) {
                            String[] data = s.split(",");
                            collector.collect(new Tuple2<>(data[1], Long.parseLong(data[2])));
                        }
                    }
                });
        DataStream<Tuple2<String, Long>> counts = dataStream.keyBy(0)
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> collector) throws Exception {
                        long sum = 0;
                        int count = 0;
                        for (Tuple2<String, Long> record : input) {
                            sum += record.f1;
                            count++;
                        }
                        Tuple2<String, Long> result = input.iterator().next();
                        result.f1 = sum / count;
                        collector.collect(result);
                    }
                });
        File out = new File(outPath);
        if (out.exists()) {
            out.delete();
        }

        counts.writeAsText(outPath, FileSystem.WriteMode.NO_OVERWRITE);
        env.setParallelism(1);
        try {
            env.execute("Flink kafka memory");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
