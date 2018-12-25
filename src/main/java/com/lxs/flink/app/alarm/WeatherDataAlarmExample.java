package com.lxs.flink.app.alarm;

import com.lxs.flink.alarm.patterns.ExcessiveHeatWarningPattern;
import com.lxs.flink.alarm.patterns.IWarningPattern;
import com.lxs.flink.alarm.warning.ExcessiveHeatWarning;
import com.lxs.flink.alarm.warning.IWarning;
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
 * 高温过热告警-温度连续两天高于41度
 * <p>
 * 极度寒冷（三天内低于-46°C）
 * 严重加热（30°C以上，连续两天）
 * 过热（高于41°C，连续两天）
 * 大风（风速在39英里/小时和110英里/小时之间）
 * 极风（风速超过110英里/小时）
 **/
public class WeatherDataAlarmExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() != null;
            }
        }).keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        });
        DataStream<LocalWeatherData> dataDataStream = keyedStream.timeWindow(Time.days(1))
                .maxBy("temperature");
        DataStream<ExcessiveHeatWarning> warningDataStream = toWarningStream(dataDataStream, new ExcessiveHeatWarningPattern());
        //简单打印
        warningDataStream.print();
        try {
            env.execute("CEP Weather Warning Example");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 将DataStream 转换为告警流
     **/
    private static <TWarningType extends IWarning> DataStream<TWarningType> toWarningStream(DataStream<LocalWeatherData> dataDataStream,
                                                                                            IWarningPattern<LocalWeatherData, TWarningType> pattern) {
        PatternStream<LocalWeatherData> patternStream = CEP.pattern(dataDataStream.keyBy(new KeySelector<LocalWeatherData, String>() {
            @Override
            public String getKey(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getStation().getWban();
            }
        }), pattern.getEventPattern());
        DataStream<TWarningType> selectStream = patternStream.select(new PatternSelectFunction<LocalWeatherData, TWarningType>() {
            @Override
            public TWarningType select(Map<String, List<LocalWeatherData>> map) throws Exception {
                return pattern.create(map);
            }
        }, new GenericTypeInfo<>(pattern.getWarningTargetType()));
        return selectStream;
    }
}
