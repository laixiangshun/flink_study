package com.lxs.flink.pgsql.mapping;

import com.lxs.flink.pgsql.model.LocalWeatherData;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataMapping extends AbstractMapping<LocalWeatherData> {

    public LocalWeatherDataMapping(String schemaName, String tableName) {
        super(schemaName, tableName);
        mapVarChar("wban", LocalWeatherData::getWban);
        mapTimeStamp("dateTime", LocalWeatherData::getDateTime);
        mapFloat("temperature", LocalWeatherData::getTemperature);
        mapFloat("windSpeed", LocalWeatherData::getWindSpeed);
        mapFloat("stationPressure", LocalWeatherData::getStationPressure);
        mapVarChar("skyCondition", LocalWeatherData::getSkyCondition);
    }
}
