package com.lxs.flink.csv.parser;

import com.lxs.flink.csv.mapping.LocalWeatherDataMapper;
import com.lxs.flink.csv.mapping.StationMapper;
import com.lxs.flink.csv.model.LocalWeatherData;
import com.lxs.flink.csv.model.Station;
import de.bytefish.jtinycsvparser.CsvParser;
import de.bytefish.jtinycsvparser.CsvParserOptions;
import de.bytefish.jtinycsvparser.tokenizer.StringSplitTokenizer;

/**
 * CSV 解析器
 **/
public class Parsers {

    public static CsvParser<Station> stationCsvParser() {
        return new CsvParser<>(new CsvParserOptions(true,
                new StringSplitTokenizer("\\|", true)),
                new StationMapper(Station::new));
    }

    public static CsvParser<LocalWeatherData> weatherDataCsvParser() {
        return new CsvParser<>(new CsvParserOptions(true,
                new StringSplitTokenizer(",", true)),
                new LocalWeatherDataMapper(LocalWeatherData::new));
    }

}
