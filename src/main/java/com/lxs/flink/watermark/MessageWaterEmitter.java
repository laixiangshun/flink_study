package com.lxs.flink.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 根据Kafka的消息确定Flink的水位
 *
 * @author lxs
 **/
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String s, long l) {
        if (s != null && s.contains(",")) {
            String[] parts = s.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    @Override
    public long extractTimestamp(String s, long l) {
        if (s != null && s.contains(",")) {
            String[] parts = s.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0;
    }
}
