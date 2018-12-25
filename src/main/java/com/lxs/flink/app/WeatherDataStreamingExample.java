package com.lxs.flink.app;

import com.lxs.flink.elastic.converter.LocalWeatherDataConverter;
import com.lxs.flink.model.LocalWeatherData;
import com.lxs.flink.sink.elastic.LocalWeatherDataElasticSearchSink;
import com.lxs.flink.sink.pgsql.LocalWeatherDataPostgresSink;
import com.lxs.flink.source.csv.LocalWeatherDataSourceFunction;
import com.lxs.flink.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
@Slf4j
public class WeatherDataStreamingExample {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(50, CheckpointingMode.AT_LEAST_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String csvStationDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503station.txt";
        final String csvLocalWeatherDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503hourly_sorted.txt";

        DataStream<LocalWeatherData> localWeatherDataDataStream = env.addSource(new LocalWeatherDataSourceFunction(csvStationDataFilePath, csvLocalWeatherDataFilePath))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LocalWeatherData>() {
                    @Override
                    public long extractAscendingTimestamp(LocalWeatherData localWeatherData) {
                        Date date = DateUtils.from(localWeatherData.getDate(), localWeatherData.getTime(), ZoneOffset.ofHours(0));
                        return date.getTime();
                    }
                });
        KeyedStream<LocalWeatherData, String> keyedStream = localWeatherDataDataStream.filter(new FilterFunction<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) {
                return localWeatherData.getTemperature() != null;
            }
        }).keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) {
                return localWeatherData.getStation().getWban();
            }
        });

        DataStream<LocalWeatherData> maxTemperaturePerDay = keyedStream.timeWindow(Time.days(1)).maxBy("temperature");

        //转变为elastic流
        DataStream<com.lxs.flink.elastic.model.LocalWeatherData> elasticDailyMaxTemperature = maxTemperaturePerDay
                .map(new MapFunction<LocalWeatherData, com.lxs.flink.elastic.model.LocalWeatherData>() {
                    @Override
                    public com.lxs.flink.elastic.model.LocalWeatherData map(LocalWeatherData localWeatherData) {
                        return LocalWeatherDataConverter.convert(localWeatherData);
                    }
                });

        //转换为pgsql流
//        DataStream<com.lxs.flink.pgsql.model.LocalWeatherData> pgsqlDailyMaxTemperature = maxTemperaturePerDay.map(new MapFunction<LocalWeatherData, com.lxs.flink.pgsql.model.LocalWeatherData>() {
//            @Override
//            public com.lxs.flink.pgsql.model.LocalWeatherData map(LocalWeatherData localWeatherData) throws Exception {
//                return com.lxs.flink.pgsql.converter.LocalWeatherDataConverter.convert(localWeatherData);
//            }
//        });
        //将结果输出到elasticSearch中
        elasticDailyMaxTemperature.addSink(new LocalWeatherDataElasticSearchSink("192.168.20.48", 9300, 100));
        //将结果输出到pgsql中
//        pgsqlDailyMaxTemperature.addSink(new LocalWeatherDataPostgresSink(URI.create("postgres://philipp:test_pwd@127.0.0.1:5432/sampledb"), 1000));
        try {
            env.execute("Max Temperature By Day example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
