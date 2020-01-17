package io.zealab.kvaft.core;

import io.zealab.kvaft.util.IdGenerator;

public class RequestId {

    private long value;

    private long createTime;

    public RequestId(long value, long createTime) {
        this.value = value;
        this.createTime = createTime;
    }

    public long getValue() {
        return this.value;
    }

    public long getCreateTime() {
        return createTime;
    }

    public static RequestId create() {
        long requestId = IdGenerator.genId();
        long createTime = System.currentTimeMillis();
        return new RequestId(requestId, createTime);
    }
}
