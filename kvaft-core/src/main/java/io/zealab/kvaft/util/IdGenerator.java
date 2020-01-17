package io.zealab.kvaft.util;

import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator {

    private final static AtomicLong requestId = new AtomicLong();

    public static long genId() {
        return requestId.getAndIncrement();
    }
}
