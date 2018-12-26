package com.lxs.flink.alarm.warning;

import com.lxs.flink.model.LocalWeatherData;

/**
 * 极度寒冷告警模式
 **/
public class ExtremeColdWarning implements IWarning {
    private LocalWeatherData localWeatherData0;
    private LocalWeatherData localWeatherData1;
    private LocalWeatherData localWeatherData2;

    public ExtremeColdWarning(LocalWeatherData localWeatherData0, LocalWeatherData localWeatherData1, LocalWeatherData localWeatherData2) {
        this.localWeatherData0 = localWeatherData0;
        this.localWeatherData1 = localWeatherData1;
        this.localWeatherData2 = localWeatherData2;
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

    public LocalWeatherData getLocalWeatherData2() {
        return localWeatherData2;
    }

    public void setLocalWeatherData2(LocalWeatherData localWeatherData2) {
        this.localWeatherData2 = localWeatherData2;
    }

    @Override
    public String toString() {
        return String.format("Extreme Cold (WBAN=%s,First Measurement=%s,Second Measurement=%s,Third Measurement=%s)",
                localWeatherData0.getStation().getWban(),
                getEventSummary(localWeatherData0),
                getEventSummary(localWeatherData1),
                getEventSummary(localWeatherData2));
    }

    private String getEventSummary(LocalWeatherData localWeatherData) {
        return String.format("Date=%s,Time =%s,Temperature=%f", localWeatherData.getDate(), localWeatherData.getTime(), localWeatherData.getTemperature());
    }
}
