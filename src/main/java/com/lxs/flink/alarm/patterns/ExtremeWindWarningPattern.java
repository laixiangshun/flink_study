package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.ExtremeWindWarning;
import com.lxs.flink.model.LocalWeatherData;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.List;
import java.util.Map;

/**
 * 疾风告警模式
 * 风速大于110
 **/
public class ExtremeWindWarningPattern implements IWarningPattern<LocalWeatherData, ExtremeWindWarning> {
    public ExtremeWindWarningPattern() {
    }

    @Override
    public ExtremeWindWarning create(Map<String, List<LocalWeatherData>> pattern) {
        LocalWeatherData first_event = pattern.get("First Event").get(0);
        return new ExtremeWindWarning(first_event);
    }

    @Override
    public Pattern<LocalWeatherData, ?> getEventPattern() {
        return Pattern.<LocalWeatherData>begin("First Event").where(new SimpleCondition<LocalWeatherData>() {
            @Override
            public boolean filter(LocalWeatherData localWeatherData) throws Exception {
                return localWeatherData.getWindSpeed() > 110.0f;
            }
        });
    }

    @Override
    public Class<ExtremeWindWarning> getWarningTargetType() {
        return ExtremeWindWarning.class;
    }
}
