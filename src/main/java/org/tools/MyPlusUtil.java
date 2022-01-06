package org.tools;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.schedule.IgniteScheduleProcessor;

/**
 * plus 的工具类
 * */
public class MyPlusUtil {

    /**
     * 输入 ignite 获取 GridKernalContext
     * @param ignite 输入 ignite 对象
     * @return ignite 的 GridKernalContext
     * */
    public static GridKernalContext getGridKernalContext(final Ignite ignite)
    {
        return ((IgniteKernal) ignite).context();
    }

    /**
     * 获取 IgniteScheduleProcessor
     * */
    public static IgniteScheduleProcessor getIgniteScheduleProcessor(final Ignite ignite)
    {
        return (IgniteScheduleProcessor)getGridKernalContext(ignite).schedule();
    }
}















































