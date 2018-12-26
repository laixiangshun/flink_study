package com.lxs.flink.alarm.warning;

import com.lxs.flink.model.LocalWeatherData;

/**
 * 大风告警
 **/
public class BigWindWarning implements IWarning {

    private LocalWeatherData localWeatherData;

    public BigWindWarning(LocalWeatherData localWeatherData) {
        this.localWeatherData = localWeatherData;
    }

    public LocalWeatherData getLocalWeatherData() {
        return localWeatherData;
    }

    public void setLocalWeatherData(LocalWeatherData localWeatherData) {
        this.localWeatherData = localWeatherData;
    }

    @Override
    public String toString() {
        return String.format("Big wind alarm （WBAN=%s,First Measurement = %s）", localWeatherData.getStation().getWban(),
                getEventSummary(localWeatherData));
    }

    private String getEventSummary(LocalWeatherData localWeatherData) {
        return String.format("Date=%s,Time=%s,WindSpeed=%f", localWeatherData.getDate(), localWeatherData.getTime(), localWeatherData.getWindSpeed());
    }
}
