package com.lxs.flink.utils;


import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * 获取可用内存信息的工具类
 **/
public class MemoryUsageExtrator {

    private static OperatingSystemMXBean mxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static long currentFreeMemorySizeInBytes() {
        return mxBean.getFreePhysicalMemorySize();
    }
}
