package com.lxs.flink.app.alarm;

import com.lxs.flink.alarm.patterns.BigWindAlarmPattern;
import com.lxs.flink.alarm.warning.BigWindWarning;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 大风告警
 * 风速在39和110之间
 **/
public class BigWindAlarmExample {

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
        DataStream<LocalWeatherData> outputStream = dataDataStream.filter(new FilterFunction<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getWindSpeed() != null;
            }
        }).keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }).timeWindow(Time.days(1)).maxBy("temperature");

        BigWindAlarmPattern bigWindAlarmPattern = new BigWindAlarmPattern();

        DataStream<BigWindWarning> bigWindWarningDataStream = CEP.pattern(outputStream.keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }), bigWindAlarmPattern.getEventPattern()).select(new PatternSelectFunction<LocalWeatherData, BigWindWarning>() {
            @Override
            public BigWindWarning select(Map<String, List<LocalWeatherData>> map) throws Exception {
                return bigWindAlarmPattern.create(map);
            }
        }, new GenericTypeInfo<>(bigWindAlarmPattern.getWarningTargetType()));

        bigWindWarningDataStream.print();

        try {
            env.execute("big wind alarm");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
