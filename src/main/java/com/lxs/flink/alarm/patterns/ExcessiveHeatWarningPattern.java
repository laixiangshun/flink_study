package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.ExcessiveHeatWarning;
import com.lxs.flink.model.LocalWeatherData;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 过热告警-至少连续两天温度高于41度
 **/
public class ExcessiveHeatWarningPattern implements IWarningPattern<LocalWeatherData, ExcessiveHeatWarning> {

    public ExcessiveHeatWarningPattern() {
    }

    @Override
    public ExcessiveHeatWarning create(Map<String, List<LocalWeatherData>> pattern) {
        LocalWeatherData first = pattern.get("First Event").get(0);
        LocalWeatherData second = pattern.get("Second Event").get(0);
        return new ExcessiveHeatWarning(first, second);
    }

    @Override
    public Pattern<LocalWeatherData, ?> getEventPattern() {
        return Pattern.<LocalWeatherData>begin("First Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() >= 41.0f;
            }
        }).next("Second Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() >= 41.0f;
            }
        }).within(Time.days(2));
    }

    @Override
    public Class<ExcessiveHeatWarning> getWarningTargetType() {
        return ExcessiveHeatWarning.class;
    }
}
