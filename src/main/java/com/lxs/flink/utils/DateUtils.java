package com.lxs.flink.utils;

import java.time.*;
import java.util.Date;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public class DateUtils {

    public static Date from(LocalDate localDate, LocalTime localTime, ZoneOffset zoneOffset) {
        LocalDateTime localDateTime = localDate.atTime(localTime);
        return from(localDateTime, zoneOffset);
    }

    public static Date from(LocalDateTime localDateTime, ZoneOffset zoneOffset) {
        OffsetDateTime offsetDateTime = localDateTime.atOffset(zoneOffset);
        return Date.from(offsetDateTime.toInstant());
    }
}
