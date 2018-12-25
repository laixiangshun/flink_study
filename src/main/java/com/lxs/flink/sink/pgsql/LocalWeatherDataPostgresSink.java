package com.lxs.flink.sink.pgsql;

import com.lxs.flink.pgsql.mapping.LocalWeatherDataMapping;
import com.lxs.flink.pgsql.model.LocalWeatherData;
import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.PgBulkInsert;

import java.net.URI;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataPostgresSink extends BasePostgresSink<LocalWeatherData> {

    public LocalWeatherDataPostgresSink(URI dataBaseUri, int bulkSize) {
        super(dataBaseUri, bulkSize);
    }

    @Override
    protected IPgBulkInsert<LocalWeatherData> getBulkInsert() {
        return new PgBulkInsert<LocalWeatherData>(new LocalWeatherDataMapping("sample", "weather_data"));
    }
}
