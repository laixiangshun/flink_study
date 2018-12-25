package com.lxs.flink.source.converter;

import com.lxs.flink.model.GeoLocation;
import com.lxs.flink.model.LocalWeatherData;
import com.lxs.flink.model.Station;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataConverter {

    public static LocalWeatherData convert(com.lxs.flink.csv.model.LocalWeatherData localWeatherData, com.lxs.flink.csv.model.Station csvStation) {
        LocalDate date = localWeatherData.getDate();
        LocalTime time = localWeatherData.getTime();
        String skyCondition = localWeatherData.getSkyCondition();
        Float stationPressure = localWeatherData.getStationPressure();
        Float temperature = localWeatherData.getDryBulbCelsius();
        Float windSpeed = localWeatherData.getWindSpeed();

        // Convert the Station data:
        Station station = convert(csvStation);
        return new LocalWeatherData(station, date, time, temperature, windSpeed, stationPressure, skyCondition);
    }

    public static Station convert(com.lxs.flink.csv.model.Station csvStation) {
        String wban = csvStation.getWban();
        String name = csvStation.getName();
        String state = csvStation.getState();
        String location = csvStation.getLocation();
        Integer timeZone = csvStation.getTimeZone();
        GeoLocation geoLocation = new GeoLocation(csvStation.getLatitude(), csvStation.getLongitude());
        return new Station(wban, name, state, location, timeZone, geoLocation);
    }
}
