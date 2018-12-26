package com.lxs.flink.app.alarm;

import com.lxs.flink.alarm.patterns.HighTemperaturePattern;
import com.lxs.flink.alarm.patterns.IWarningPattern;
import com.lxs.flink.alarm.warning.HighTemperatureWarning;
import com.lxs.flink.model.LocalWeatherData;
import com.lxs.flink.source.csv.LocalWeatherDataSourceFunction;
import com.lxs.flink.utils.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * CEP 处理复杂事件
 * 高温告警----连续两天温度高于30度
 **/
public class HighTemperatureAlarmExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        evn.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String csvStationDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503station.txt";
        final String csvLocalWeatherDataFilePath = "C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503hourly_sorted.txt";
        DataStream<LocalWeatherData> dataDataStream = evn.addSource(new LocalWeatherDataSourceFunction(csvStationDataFilePath, csvLocalWeatherDataFilePath))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LocalWeatherData>() {
                    @Override
                    public long extractAscendingTimestamp(LocalWeatherData localWeatherData) {
                        Date from = DateUtils.from(localWeatherData.getDate(), localWeatherData.getTime(), ZoneOffset.ofHours(0));
                        return from.getTime();
                    }
                });
        KeyedStream<LocalWeatherData, String> keyedStream = dataDataStream.filter(new FilterFunction<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() != null;
            }
        }).keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        });
        DataStream<LocalWeatherData> temperature = keyedStream.timeWindow(Time.days(1)).maxBy("temperature");
        DataStream<HighTemperatureWarning> highTemperatureWarningDataStream = toWarningStream(temperature, new HighTemperaturePattern());
        highTemperatureWarningDataStream.print();
        try {
            evn.execute("high temperature alarm");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 将DataStream转换为告警流
     **/
    private static <IWarning extends com.lxs.flink.alarm.warning.IWarning> DataStream<IWarning> toWarningStream(DataStream<LocalWeatherData> inputStream,
                                                                                                                IWarningPattern<LocalWeatherData, IWarning> pattern) {
        PatternStream<LocalWeatherData> patternStream = CEP.pattern(inputStream.keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }), pattern.getEventPattern());
        DataStream<IWarning> outputStreamOperator = patternStream.select(new PatternSelectFunction<LocalWeatherData, IWarning>() {
            @Override
            public IWarning select(Map<String, List<LocalWeatherData>> map) throws Exception {
                return pattern.create(map);
            }
        }, new GenericTypeInfo<>(pattern.getWarningTargetType()));
        return outputStreamOperator;
    }
}
