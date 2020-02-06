package io.zealab.kvaft.rpc.client;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.zealab.kvaft.core.*;
import io.zealab.kvaft.rpc.protoc.codec.KvaftDefaultCodecHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
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

    public void registerReplicator(Endpoint endpoint, Client client) {
        // expose client instance for another thread (maybe)
        Replicator replicator = new Replicator(endpoint, client, ReplicatorState.CONNECTED);
        registerReplicator(replicator);
    }

    public void registerReplicator(Replicator replicator) {
        final Lock wl = replicatorLock.writeLock();
        try {
            wl.lock();
            activeReplicators.putIfAbsent(replicator.nodeId(), replicator);
        } finally {
            wl.unlock();
        }
    }

    public void removeReplicator(Endpoint endpoint) {
        final Lock wl = replicatorLock.writeLock();
        try {
            wl.lock();
            activeReplicators.remove(endpoint.toString());
        } finally {
            wl.unlock();
        }
    }

    public Replicator getReplicator(String nodeId) {
        final Lock rl = replicatorLock.readLock();
        try {
            rl.lock();
            return activeReplicators.get(nodeId);
        } finally {
            rl.unlock();
        }
    }

    public static class ReplicatorChannelPoolHandler implements ChannelPoolHandler {

        @Override
        public void channelReleased(Channel ch) throws Exception {
            log.debug("ReplicatorChannelPoolHandler => channel released");
        }

        @Override
        public void channelAcquired(Channel ch) throws Exception {
            log.debug("ReplicatorChannelPoolHandler => channel acquired");
        }

        @Override
        public void channelCreated(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
            pipeline.addLast("codec", new KvaftDefaultCodecHandler());
            pipeline.addLast("responseHandler", new ResponseHandler());
        }
    }
}
