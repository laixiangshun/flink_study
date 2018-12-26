package com.lxs.flink.alarm.warning;

import com.lxs.flink.model.LocalWeatherData;

/**
 * 高温告警模板
 **/
public class HighTemperatureWarning implements IWarning {
    private LocalWeatherData localWeatherData0;

    private LocalWeatherData localWeatherData1;

    public HighTemperatureWarning(LocalWeatherData localWeatherData0, LocalWeatherData localWeatherData1) {
        this.localWeatherData0 = localWeatherData0;
        this.localWeatherData1 = localWeatherData1;
    }

    public LocalWeatherData getLocalWeatherData0() {
        return localWeatherData0;
    }

    public void setLocalWeatherData0(LocalWeatherData localWeatherData0) {
        this.localWeatherData0 = localWeatherData0;
    }

    public LocalWeatherData getLocalWeatherData1() {
        return localWeatherData1;
    }

    public void setLocalWeatherData1(LocalWeatherData localWeatherData1) {
        this.localWeatherData1 = localWeatherData1;
    }

    @Override
    public String toString() {
        return String.format("HighTemperature,(WBAN=%s,First Measurement = (%s),Second Measurement = (%s))",
                localWeatherData0.getStation().getWban(),
                getEventSummary(localWeatherData0),
                getEventSummary(localWeatherData1));
    }

    private String getEventSummary(LocalWeatherData localWeatherData) {
        return String.format("Date = %s,Time = %s,Temperature = %f", localWeatherData.getDate(), localWeatherData.getTime(), localWeatherData.getTemperature());
    }
}
