package com.lxs.flink.alarm.warning;

import com.lxs.flink.model.LocalWeatherData;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/25
 **/
public class ExcessiveHeatWarning implements IWarning {

    private final LocalWeatherData localWeatherData0;
    private final LocalWeatherData localWeatherData1;


    public LocalWeatherData getLocalWeatherData0() {
        return localWeatherData0;
    }

    public LocalWeatherData getLocalWeatherData1() {
        return localWeatherData1;
    }

    public ExcessiveHeatWarning(LocalWeatherData localWeatherData0, LocalWeatherData localWeatherData1) {
        this.localWeatherData0 = localWeatherData0;
        this.localWeatherData1 = localWeatherData1;
    }

    @Override
    public String toString() {
        return String.format("ExcessiveHeatWarning (Wabn=%s),first Measurement=(%s),second Measurement=(%s)",
                localWeatherData0.getStation().getWban(),
                getMeasurement(localWeatherData0),
                getMeasurement(localWeatherData1));
    }

    private String getMeasurement(LocalWeatherData localWeatherData) {
        return String.format("Date=%s,Time=%s,Temperature=%f", localWeatherData.getDate(), localWeatherData.getTime()
                , localWeatherData.getTemperature());
    }
}
