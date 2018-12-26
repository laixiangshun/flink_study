package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.BigWindWarning;
import com.lxs.flink.model.LocalWeatherData;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.List;
import java.util.Map;

/**
 * 大风告警模式
 * 风速在39和110之间
 **/
public class BigWindAlarmPattern implements IWarningPattern<LocalWeatherData, BigWindWarning> {
    public BigWindAlarmPattern() {
    }

    @Override
    public BigWindWarning create(Map<String, List<LocalWeatherData>> pattern) {
        LocalWeatherData first_event = pattern.get("First Event").get(0);
        return new BigWindWarning(first_event);
    }

    @Override
    public Pattern<LocalWeatherData, ?> getEventPattern() {
        return Pattern.<LocalWeatherData>begin("First Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getWindSpeed() >= 39.0f && localWeatherData.getWindSpeed() <= 110.0f;
            }
        });
    }

    @Override
    public Class<BigWindWarning> getWarningTargetType() {
        return BigWindWarning.class;
    }
}
