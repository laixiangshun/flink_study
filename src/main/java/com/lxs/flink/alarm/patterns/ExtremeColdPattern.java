package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.ExtremeColdWarning;
import com.lxs.flink.model.LocalWeatherData;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 极度寒冷告警模式
 **/
public class ExtremeColdPattern implements IWarningPattern<LocalWeatherData, ExtremeColdWarning> {
    public ExtremeColdPattern() {
    }

    @Override
    public ExtremeColdWarning create(Map<String, List<LocalWeatherData>> pattern) {
        LocalWeatherData first_event = pattern.get("First Event").get(0);
        LocalWeatherData second_event = pattern.get("Second Event").get(0);
        LocalWeatherData third_event = pattern.get("Third Event").get(0);
        return new ExtremeColdWarning(first_event, second_event, third_event);
    }

    @Override
    public Pattern<LocalWeatherData, ?> getEventPattern() {
        return Pattern.<LocalWeatherData>begin("First Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() <= -46.0f;
            }
        }).next("Second Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() <= -46.0f;
            }
        }).next("Third Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getTemperature() <= -46.0f;
            }
        }).within(Time.days(3));
    }

    @Override
    public Class<ExtremeColdWarning> getWarningTargetType() {
        return ExtremeColdWarning.class;
    }
}
