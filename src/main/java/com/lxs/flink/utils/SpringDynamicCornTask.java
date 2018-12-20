package com.lxs.flink.utils;


import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

/**
 * 动态更改cron表达式
 **/
@Lazy(false)
@Component
@EnableScheduling
public class SpringDynamicCornTask implements SchedulingConfigurer {
    private static final String cron = "0 0/10 * * * ?";

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        scheduledTaskRegistrar.addTriggerTask(() -> System.out.println(System.currentTimeMillis()), triggerContext -> {
            CronTrigger cronTrigger = new CronTrigger(cron);
            return cronTrigger.nextExecutionTime(triggerContext);
        });
    }
}
