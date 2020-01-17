package io.zealab.kvaft.rpc.client;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
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

    public void registerActiveReplicator(Replicator replicator) {
        final Lock wl = replicatorLock.writeLock();
        wl.lock();
        try {
            activeReplicators.putIfAbsent(replicator.nodeId(), replicator);
        } finally {
            wl.unlock();
        }
    }

    public Replicator getReplicator(String nodeId) {
        final Lock rl = replicatorLock.readLock();
        rl.lock();
        try {
            return activeReplicators.get(nodeId);
        } finally {
            rl.unlock();
        }
    }

    public static class ReplicatorChannelPoolHandler implements ChannelPoolHandler {

        @Override
        public void channelReleased(Channel ch) throws Exception {

        }

        @Override
        public void channelAcquired(Channel ch) throws Exception {

        }

        @Override
        public void channelCreated(Channel ch) throws Exception {

        }
    }
}
