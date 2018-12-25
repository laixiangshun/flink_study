package com.lxs.flink.csv.sorting;

import com.lxs.flink.csv.model.LocalWeatherData;
import com.lxs.flink.csv.model.Station;
import com.lxs.flink.csv.parser.Parsers;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import org.apache.flink.table.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class PrepareWeatherData {
    public static void main(String[] args) {

        final Path csvStationDataFilePath = FileSystems.getDefault().getPath("C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503station.txt");
        final Path csvLocalWeatherDataUnsortedFilePath = FileSystems.getDefault().getPath("C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503hourly.txt");
        final Path csvLocalWeatherDataSortedFilePath = FileSystems.getDefault().getPath("C:\\Users\\hasee\\Desktop\\QCLCD201503\\201503hourly_sorted.txt");

        Map<String, Station> stationMap = getStationMap(csvStationDataFilePath);
        List<Integer> indices;
        Comparator<OffsetDateTime> byMeasurementTime = OffsetDateTime::compareTo;
        Stream<CsvMappingResult<LocalWeatherData>> localWeatherData = getLocalWeatherData(csvLocalWeatherDataUnsortedFilePath);
        AtomicInteger currentIndex = new AtomicInteger(1);
        indices = localWeatherData.skip(1)
                .map(x -> new ImmutablePair<>(currentIndex.getAndAdd(1), x))
                .filter(x -> x.getRight().isValid())
                .map(x -> new ImmutablePair<>(x.getLeft(), x.getRight().getResult()))
                .filter(x -> stationMap.containsKey(x.getRight().getWban()))
                .map(x -> {
                    OffsetDateTime offsetDateTime = OffsetDateTime.of(x.getRight().getDate(), x.getRight().getTime(), ZoneOffset.ofHours(0));
                    return new ImmutablePair<>(x.getLeft(), offsetDateTime);
                }).sorted((x, y) -> byMeasurementTime.compare(x.getRight(), y.getRight()))
                .map(ImmutablePair::getLeft)
                .collect(Collectors.toList());
        writeSortedFile(csvLocalWeatherDataUnsortedFilePath, indices, csvLocalWeatherDataSortedFilePath);
    }

    private static void writeSortedFile(Path csvIn, List<Integer> indices, Path csvOut) {
        List<String> data;
        BufferedWriter writer = null;
        try {
            Stream<String> lines = Files.lines(csvIn, StandardCharsets.US_ASCII).skip(1);
            data = lines.collect(Collectors.toList());
            writer = Files.newBufferedWriter(csvOut);
            for (Integer line : indices) {
                writer.write(data.get(line));
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static Stream<CsvMappingResult<LocalWeatherData>> getLocalWeatherData(Path path) {
        return Parsers.weatherDataCsvParser().readFromFile(path, StandardCharsets.US_ASCII);
    }

    private static Map<String, Station> getStationMap(Path path) {
        Stream<Station> stations = getStations(path);
        return stations.collect(Collectors.groupingBy(Station::getWban))
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().get(0)));
    }

    private static Stream<Station> getStations(Path path) {
        return Parsers.stationCsvParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(CsvMappingResult::isValid)
                .map(CsvMappingResult::getResult);
    }
}
