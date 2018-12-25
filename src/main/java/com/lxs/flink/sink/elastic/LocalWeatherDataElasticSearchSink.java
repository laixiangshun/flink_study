package com.lxs.flink.sink.elastic;

import com.lxs.flink.elastic.mapping.LocalWeatherDataMapper;
import com.lxs.flink.elastic.model.LocalWeatherData;
import de.bytefish.elasticutils.elasticsearch6.mapping.IElasticSearchMapping;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataElasticSearchSink extends BaseElasticSearchSink<LocalWeatherData> {

    public LocalWeatherDataElasticSearchSink(String host, int port, int bulkSize) {
        super(host, port, bulkSize);
    }

    @Override
    protected String getIndexName() {
        return "weather_data";
    }

    @Override
    protected IElasticSearchMapping getMapping() {
        return new LocalWeatherDataMapper();
    }
}
