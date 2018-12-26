package com.lxs.flink.app.alarm;

import com.lxs.flink.alarm.patterns.ExtremeColdPattern;
import com.lxs.flink.alarm.warning.ExtremeColdWarning;
import com.lxs.flink.model.LocalWeatherData;
import com.lxs.flink.source.csv.LocalWeatherDataSourceFunction;
import com.lxs.flink.utils.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 极度寒冷告警
 * 连续三天温度低于46度
 **/
public class ExtremeColdAlarmExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final String csvStationDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503station.txt";
        final String csvLocalWeatherDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503hourly_sorted.txt";

        DataStream<LocalWeatherData> dataDataStream = env.addSource(new LocalWeatherDataSourceFunction(csvStationDataFilePath, csvLocalWeatherDataFilePath))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LocalWeatherData>() {
                    @Override
                    public long extractAscendingTimestamp(LocalWeatherData localWeatherData) {
                        Date from = DateUtils.from(localWeatherData.getDate(), localWeatherData.getTime(), ZoneOffset.ofHours(0));
                        return from.getTime();
                    }
                });
        DataStream<LocalWeatherData> outputStreamStream = dataDataStream.filter(new FilterFunction<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() != null;
            }
        }).keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }).timeWindow(Time.days(1)).maxBy("temperature");
        ExtremeColdPattern extremeColdPattern = new ExtremeColdPattern();
        DataStream<ExtremeColdWarning> warningDataStream = CEP.pattern(outputStreamStream.keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }), extremeColdPattern.getEventPattern()).select(new PatternSelectFunction<LocalWeatherData, ExtremeColdWarning>() {
            @Override
            public ExtremeColdWarning select(Map<String, List<LocalWeatherData>> map) throws Exception {
                return extremeColdPattern.create(map);
            }
        }, new GenericTypeInfo<>(extremeColdPattern.getWarningTargetType()));

        warningDataStream.print();
        try {
            env.execute("Extreme cold alarm");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
