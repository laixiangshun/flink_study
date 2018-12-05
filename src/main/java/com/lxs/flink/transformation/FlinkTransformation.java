package com.lxs.flink.transformation;

import com.lxs.flink.model.Student;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/5
 **/
public class FlinkTransformation {
    private static final String broker_list = "192.168.20.48:9092";
    private static final String quorum = "192.168.20.48:2181,192.168.20.52:2181,192.168.20.51:2181";
    private static final String topic = "metric";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker_list);
        prop.put("zookeeper.connect", quorum);
        prop.put("group.id", "student-group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop)).setParallelism(1);

        SingleOutputStreamOperator<Student> operator = map(dataStreamSource);

        operator.addSink(new PrintSinkFunction<>());
        try {
            env.execute("flink transformation");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SingleOutputStreamOperator<Student> map(DataStreamSource<String> dataStreamSource) {
        SingleOutputStreamOperator<Student> operator = dataStreamSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                Student student = mapper.readValue(s, Student.class);
                student.setAge(student.getAge() + 5);
                return student;
            }
        });
        return operator;
    }

    private static SingleOutputStreamOperator<Student> flatMap(DataStreamSource<String> dataStreamSource) {
        SingleOutputStreamOperator<Student> operator = dataStreamSource.flatMap(new FlatMapFunction<String, Student>() {
            @Override
            public void flatMap(String s, Collector<Student> collector) throws Exception {
                Student student = mapper.readValue(s, Student.class);
                if (student.getAge() % 5 == 0) {
                    collector.collect(student);
                }
            }
        });
        return operator;
    }

    private static SingleOutputStreamOperator<Student> filter(DataStreamSource<String> dataStreamSource) {
        SingleOutputStreamOperator<Student> operator = dataStreamSource.map(s -> mapper.readValue(s, Student.class)).filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student s) throws Exception {
                if (s.getAge() > 60) {
                    return true;
                }
                return false;
            }
        });
        return operator;
    }

    /**
     * 基于 key 对流进行分区
     **/
    private static SingleOutputStreamOperator<Student> keyBy(DataStreamSource<Student> dataStreamSource) {
        KeyedStream<Student, Integer> keyedStream = dataStreamSource.keyBy(new KeySelector<Student, Integer>() {

            @Override
            public Integer getKey(Student student) throws Exception {
                return student.getAge();
            }
        });
        SingleOutputStreamOperator<Student> operator = keyedStream.sum("id");
        return operator;
    }

    /**
     * Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值
     * 先将数据流进行 keyby 操作，因为执行 reduce 操作只能是 KeyedStream
     **/
    private static SingleOutputStreamOperator<Student> reduce(DataStreamSource<Student> dataStreamSource) {
        KeyedStream<Student, String> keyedStream = dataStreamSource.keyBy(new KeySelector<Student, String>() {

            @Override
            public String getKey(Student student) throws Exception {
                return student.getName();
            }
        });
        SingleOutputStreamOperator<Student> reduce = keyedStream.reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student t1, Student t2) throws Exception {
                Student student = new Student();
                student.setName(t1.getName());
                student.setPassword(t1.getPassword() + t2.getPassword());
                student.setAge((t1.getAge() + t2.getAge()) / 2);
                return student;
            }
        });
        return reduce;
    }

    /**
     * 从事件流中选择属性子集，并仅将所选元素发送到下一个处理流
     **/
    private static DataStream<Tuple2<String, String>> project(DataStreamSource<Student> dataStreamSource) {
        SingleOutputStreamOperator<Tuple4<Integer, Integer, String, String>> in = dataStreamSource.flatMap(new FlatMapFunction<Student, Tuple4<Integer, Integer, String, String>>() {
            @Override
            public void flatMap(Student student, Collector<Tuple4<Integer, Integer, String, String>> collector) throws Exception {
                collector.collect(new Tuple4<>(student.getId(), student.getAge(), student.getName(), student.getPassword()));
            }
        });
        DataStream<Tuple2<String, String>> out = in.project(2, 3);
        return out;
    }

    /**
     * 根据条件将流拆分为两个或多个流 split
     * 从拆分流中选择特定流  select
     **/
    private static DataStream<Student> split(DataStreamSource<Student> dataStreamSource) {
        SplitStream<Student> splitStream = dataStreamSource.split(new OutputSelector<Student>() {
            @Override
            public Iterable<String> select(Student student) {
                List<String> output = new ArrayList<>();
                if (student.getAge() > 50) {
                    output.add("greater than 50");
                } else {
                    output.add("less than 50");
                }
                return output;
            }
        });
        DataStream<Student> greater = splitStream.select("greater than 50");
        DataStream<Student> less = splitStream.select("less than 50");
        DataStream<Student> all = splitStream.select("greater than 50", "less than 50");
        return all;
    }

    /**
     * 通过一些 key 将同一个 window 的两个数据流 join 起来
     **/
    private static DataStream<Tuple2<String, String>> join(DataStream<Tuple2<String, String>> in1, DataStream<Tuple2<String, String>> in2) {
        return in1.join(in2).where(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> t1) throws Exception {
                return t1.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> t2) throws Exception {
                return t2.f0;
            }
        }).window(new WindowAssigner<CoGroupedStreams.TaggedUnion<Tuple2<String, String>, Tuple2<String, String>>, Window>() {
            @Override
            public Collection<Window> assignWindows(CoGroupedStreams.TaggedUnion<Tuple2<String, String>, Tuple2<String, String>> tuple2Tuple2TaggedUnion, long l, WindowAssignerContext windowAssignerContext) {
                return null;
            }

            @Override
            public Trigger<CoGroupedStreams.TaggedUnion<Tuple2<String, String>, Tuple2<String, String>>, Window> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
                return null;
            }

            @Override
            public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public boolean isEventTime() {
                return false;
            }
        }).apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> join(Tuple2<String, String> t1, Tuple2<String, String> t2) throws Exception {
                return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1);
            }
        });
    }

    /**
     * 两个或多个数据流结合在一起
     **/
    private static DataStream<String> union(DataStream<String> dataStream1, DataStream<String> dataStream2) {
        return dataStream1.union(dataStream2);
    }
}
