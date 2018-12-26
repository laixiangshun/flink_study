package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.HighTemperatureWarning;
import com.lxs.flink.model.LocalWeatherData;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 高温告警模式
 * 连续2天温度高于30度
 **/
public class HighTemperaturePattern implements IWarningPattern<LocalWeatherData, HighTemperatureWarning> {
    public HighTemperaturePattern() {
    }

    @Override
    public HighTemperatureWarning create(Map<String, List<LocalWeatherData>> pattern) {
        LocalWeatherData first_event = pattern.get("First Event").get(0);
        LocalWeatherData second_event = pattern.get("Second Event").get(0);
        return new HighTemperatureWarning(first_event, second_event);
    }

    @Override
    public Pattern<LocalWeatherData, ?> getEventPattern() {
        return Pattern.<LocalWeatherData>begin("First Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() >= 30.0f;
            }
        }).next("Second Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() >= 30.0f;
            }
        }).within(Time.days(2));
    }

    @Override
    public Class<HighTemperatureWarning> getWarningTargetType() {
        return HighTemperatureWarning.class;
    }
}
