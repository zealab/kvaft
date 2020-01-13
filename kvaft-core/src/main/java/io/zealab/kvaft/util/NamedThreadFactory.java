package io.zealab.kvaft.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NamedThreadFactory
 *
 * @author LeonWong
 */
@Slf4j
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger node = new AtomicInteger(0);

    private final boolean daemon;

    private final String name;

    private static final LogUncaughtExceptionHandler UNCAUGHT_EX_HANDLER = new LogUncaughtExceptionHandler();

    public NamedThreadFactory(String name, boolean daemon) {
        this.daemon = daemon;
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(this.daemon);
        t.setUncaughtExceptionHandler(UNCAUGHT_EX_HANDLER);
        t.setName(String.format(name, node.getAndIncrement()));
        return t;
    }

    private static final class LogUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught exception in thread {}", t, e);
        }
    }
}
