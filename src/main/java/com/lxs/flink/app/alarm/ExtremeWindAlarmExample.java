package com.lxs.flink.app.alarm;

import com.lxs.flink.alarm.patterns.ExtremeWindWarningPattern;
import com.lxs.flink.alarm.warning.ExtremeWindWarning;
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
 * 疾风告警模式
 * 风速大于110
 **/
public class ExtremeWindAlarmExample {
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
        DataStream<LocalWeatherData> keyByStream = dataDataStream.filter(new FilterFunction<LocalWeatherData>() {
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

        ExtremeWindWarningPattern extremeWindWarningPattern = new ExtremeWindWarningPattern();

        DataStream<ExtremeWindWarning> extremeWindWarningDataStream = CEP.pattern(keyByStream.keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }), extremeWindWarningPattern.getEventPattern()).select(new PatternSelectFunction<LocalWeatherData, ExtremeWindWarning>() {
            @Override
            public ExtremeWindWarning select(Map<String, List<LocalWeatherData>> map) throws Exception {
                return extremeWindWarningPattern.create(map);
            }
        }, new GenericTypeInfo<>(extremeWindWarningPattern.getWarningTargetType()));
        extremeWindWarningDataStream.print();
        try {
            env.execute("extreme wind alarm");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
