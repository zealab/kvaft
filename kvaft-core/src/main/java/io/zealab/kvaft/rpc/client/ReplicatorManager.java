package io.zealab.kvaft.rpc.client;

import com.google.common.collect.Maps;
import io.zealab.kvaft.core.Replicator;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicatorManager {

    private final Map<String, Replicator> activeReplicators = Maps.newHashMap();

    private final ReadWriteLock replicatorLock = new ReentrantReadWriteLock();

    private ReplicatorManager() {}

    private static class SingletonHolder {
        private static ReplicatorManager instance = new ReplicatorManager();
    }

    public static ReplicatorManager getInstance() {
        return SingletonHolder.instance;
    }

    public void addActiveReplicators(Replicator replicator) {
        final Lock wl = replicatorLock.writeLock();
        wl.lock();
        try {
            activeReplicators.put(replicator.nodeId(), replicator);
        } finally {
            wl.unlock();
        }
    }
}
