package com.lxs.flink.alarm.patterns;

import com.lxs.flink.alarm.warning.IWarning;
import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 告警模式
 **/
public interface IWarningPattern<TEventType, TWarningType extends IWarning> extends Serializable {

    TWarningType create(Map<String, List<TEventType>> pattern);

    Pattern<TEventType, ?> getEventPattern();

    Class<TWarningType> getWarningTargetType();
}
