package io.zealab.kvaft.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimerManager {

    private final static ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("TimerTask-%d", true));

    /**
     * schedule with fix rate
     *
     * @param command      task
     * @param initialDelay delay
     * @param period       period
     * @param unit         time unit
     *
     * @return
     */
    public static ScheduledFuture<?> scheduleWithFixRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduler.scheduleAtFixedRate(command, initialDelay, period, unit);
    }
}
