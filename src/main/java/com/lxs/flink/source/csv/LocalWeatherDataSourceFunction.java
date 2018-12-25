package com.lxs.flink.source.csv;

import com.lxs.flink.csv.parser.Parsers;
import com.lxs.flink.model.LocalWeatherData;
import com.lxs.flink.source.converter.LocalWeatherDataConverter;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 自定义 source
 **/
public class LocalWeatherDataSourceFunction implements SourceFunction<LocalWeatherData> {
    private volatile boolean isRunning = true;

    private String stationFilePath;
    private String localWeatherDataFilePath;

    public LocalWeatherDataSourceFunction(String stationFilePath, String localWeatherDataFilePath) {
        this.stationFilePath = stationFilePath;
        this.localWeatherDataFilePath = localWeatherDataFilePath;
    }

    @Override
    public void run(SourceContext<LocalWeatherData> sourceContext) {
        Path csvStationPath = FileSystems.getDefault().getPath(stationFilePath);
        Path csvLocalWeatherDataPath = FileSystems.getDefault().getPath(localWeatherDataFilePath);
        try (Stream<LocalWeatherData> localWeatherData = getLocalWeatherData(csvStationPath, csvLocalWeatherDataPath)) {
            Iterator<LocalWeatherData> iterator = localWeatherData.iterator();
            while (isRunning && iterator.hasNext()) {
                sourceContext.collect(iterator.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Stream<LocalWeatherData> getLocalWeatherData(Path csvStationPath, Path csvLocalWeatherDataPath) {
        Map<String, com.lxs.flink.csv.model.Station> stationMap = getStationMap(csvStationPath);
        Stream<com.lxs.flink.csv.model.LocalWeatherData> localWeatherDataStream = getLocalWeatherData(csvLocalWeatherDataPath);
        return localWeatherDataStream.filter(x -> stationMap.containsKey(x.getWban()))
                .map(x -> {
                    com.lxs.flink.csv.model.Station station = stationMap.get(x.getWban());
                    return LocalWeatherDataConverter.convert(x, station);
                });
    }

    private Stream<com.lxs.flink.csv.model.LocalWeatherData> getLocalWeatherData(Path weatherDataPath) {
        return Parsers.weatherDataCsvParser().readFromFile(weatherDataPath, StandardCharsets.US_ASCII)
                .filter(CsvMappingResult::isValid)
                .map(CsvMappingResult::getResult);
    }

    private Map<String, com.lxs.flink.csv.model.Station> getStationMap(Path path) {
        Stream<com.lxs.flink.csv.model.Station> stationStream = getStations(path);
        return stationStream.collect(Collectors.toMap(com.lxs.flink.csv.model.Station::getWban, Function.identity(), (val1, val2) -> val2));
    }

    private Stream<com.lxs.flink.csv.model.Station> getStations(Path path) {
        return Parsers.stationCsvParser()
                .readFromFile(path, StandardCharsets.US_ASCII)
                .filter(CsvMappingResult::isValid)
                .map(CsvMappingResult::getResult);
    }
}
