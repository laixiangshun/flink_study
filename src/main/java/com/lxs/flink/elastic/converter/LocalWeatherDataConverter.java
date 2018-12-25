// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.lxs.flink.elastic.converter;

import com.lxs.flink.elastic.model.GeoLocation;
import com.lxs.flink.elastic.model.LocalWeatherData;
import com.lxs.flink.elastic.model.Station;
import com.lxs.flink.utils.DateUtils;

import java.time.ZoneOffset;

public class LocalWeatherDataConverter {

    public static LocalWeatherData convert(com.lxs.flink.model.LocalWeatherData modelLocalWeatherData) {

        LocalWeatherData elasticLocalWeatherData = new LocalWeatherData();

        elasticLocalWeatherData.dateTime = DateUtils.from(modelLocalWeatherData.getDate(), modelLocalWeatherData.getTime(), ZoneOffset.ofHours(modelLocalWeatherData.getStation().getTimeZone()));
        elasticLocalWeatherData.skyCondition = modelLocalWeatherData.getSkyCondition();
        elasticLocalWeatherData.stationPressure = modelLocalWeatherData.getStationPressure();
        elasticLocalWeatherData.temperature = modelLocalWeatherData.getTemperature();
        elasticLocalWeatherData.windSpeed = modelLocalWeatherData.getWindSpeed();

        // Convert the Station data:
        elasticLocalWeatherData.station = convert(modelLocalWeatherData.getStation());

        return elasticLocalWeatherData;
    }

    private static Station convert(com.lxs.flink.model.Station modelStation) {
        Station elasticStation = new Station();

        elasticStation.wban = modelStation.getWban();
        elasticStation.name = modelStation.getName();
        elasticStation.state = modelStation.getState();
        elasticStation.location = modelStation.getLocation();

        // Convert the GeoLocation:
        elasticStation.geoLocation = convert(modelStation.getGeoLocation());

        return elasticStation;
    }

    private static GeoLocation convert(com.lxs.flink.model.GeoLocation modelGeoLocation) {
        GeoLocation elasticGeoLocation = new GeoLocation();

        elasticGeoLocation.lat = modelGeoLocation.getLat();
        elasticGeoLocation.lon = modelGeoLocation.getLon();

        return elasticGeoLocation;
    }

}
