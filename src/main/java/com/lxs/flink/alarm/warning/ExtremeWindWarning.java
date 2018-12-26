package com.lxs.flink.alarm.warning;

import com.lxs.flink.model.LocalWeatherData;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/26
 **/
public class ExtremeWindWarning implements IWarning {
    private LocalWeatherData localWeatherData;

    public ExtremeWindWarning(LocalWeatherData localWeatherData) {
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
        return String.format("ExtremeWindWarning(WABN=%s,First Measurement =%s)", localWeatherData.getStation().getWban(), getEventSummary(localWeatherData));
    }

    private String getEventSummary(LocalWeatherData localWeatherData) {
        return String.format("Date = %s,Time = %s,WindSpeed=%f", localWeatherData.getDate(), localWeatherData.getTime(), localWeatherData.getWindSpeed());
    }
}
