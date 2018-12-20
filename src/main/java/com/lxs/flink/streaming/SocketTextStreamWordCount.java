package com.lxs.flink.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * 从socket套接字 监控输入
 **/
public class SocketTextStreamWordCount {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(1);
        }
//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String ip = tool.get("ip");
//        int port = Integer.parseInt(tool.get("port"));
        String ip = args[0];
        Integer port = Integer.valueOf(args[1]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //获取数据
        DataStream<String> stream = env.socketTextStream(ip, port);
        //计数
//        DataStream<Tuple2<String, Integer>> streamOperator = streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
//            String[] tokens = s.toLowerCase().split("\W+");
//            for (String token : tokens) {
//                if (token.length() > 1) {
//                    collector.collect(new Tuple2<>(token, 1));
//                }
//            }
//        }).keyBy(0).sum(1);
//        DataStream<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
//                .keyBy(0)
//                .sum(1)
//                .timeWindow(Time.seconds(5));

        DataStream<WordWithCount> sum = stream.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) {
                String[] words = s.split("\\W+");
                for (String word : words) {
                    collector.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy(s -> s.word)
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce((WordWithCount c1, WordWithCount c2) -> new WordWithCount(c1.word, c1.count + c2.count));

        // print the results with a single thread, rather than in parallel
        sum.print().setParallelism(1);
        try {
            env.execute("Socket Window WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    private static class WordWithCount {
        private String word;
        private Integer count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
