package com.lxs.flink.elastic.mapping;

import de.bytefish.elasticutils.elasticsearch6.mapping.BaseElasticSearchMapping;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.*;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class LocalWeatherDataMapper extends BaseElasticSearchMapping {

    private static final String INDEX_TYPE = "document";

    public LocalWeatherDataMapper() {
        super(INDEX_TYPE, Version.V_6_5_1);
    }

    @Override
    protected void configureRootObjectBuilder(RootObjectMapper.Builder builder) {
        builder.add(new DateFieldMapper.Builder("dateTime"))
                .add(new NumberFieldMapper.Builder("temperature", NumberFieldMapper.NumberType.FLOAT))
                .add(new NumberFieldMapper.Builder("windSpeed", NumberFieldMapper.NumberType.FLOAT))
                .add(new NumberFieldMapper.Builder("stationPressure", NumberFieldMapper.NumberType.FLOAT))
                .add(new TextFieldMapper.Builder("skyCondition"))
                .add(new ObjectMapper.Builder("station")
                        .add(new TextFieldMapper.Builder("wban"))
                        .add(new TextFieldMapper.Builder("name"))
                        .add(new TextFieldMapper.Builder("state"))
                        .add(new TextFieldMapper.Builder("location"))
                        .add(new GeoPointFieldMapper.Builder("coordinates"))
                        .nested(ObjectMapper.Nested.newNested(true, false))
                );
    }
}
