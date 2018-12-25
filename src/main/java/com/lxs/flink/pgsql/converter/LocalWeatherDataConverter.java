package com.lxs.flink.pgsql.converter;

import com.lxs.flink.pgsql.model.LocalWeatherData;

import java.time.LocalDateTime;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataConverter {

    public static LocalWeatherData convert(com.lxs.flink.model.LocalWeatherData modelLocalWeatherData) {
        String wban = modelLocalWeatherData.getStation().getWban();
        LocalDateTime dateTime = modelLocalWeatherData.getDate().atTime(modelLocalWeatherData.getTime());
        Float temperature = modelLocalWeatherData.getTemperature();
        Float windSpeed = modelLocalWeatherData.getWindSpeed();
        Float stationPressure = modelLocalWeatherData.getStationPressure();
        String skyCondition = modelLocalWeatherData.getSkyCondition();

        return new LocalWeatherData(wban, dateTime, temperature, windSpeed, stationPressure, skyCondition);
    }
}
