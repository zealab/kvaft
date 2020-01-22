package io.zealab.kvaft.rpc.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.*;
import io.zealab.kvaft.rpc.ChannelProcessor;
import io.zealab.kvaft.rpc.protoc.codec.KvaftDefaultCodecHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class ReplicatorManager implements Scanner {

    private final Map<String, Replicator> activeReplicators = Maps.newHashMap();

    private final Map<String, ChannelProcessor> registry = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io.zealab.kvaft";

    private final ReadWriteLock replicatorLock = new ReentrantReadWriteLock();

    private volatile Node node = null;

    private ReplicatorManager() {}

    private static class SingletonHolder {
        private static ReplicatorManager instance = new ReplicatorManager();
    }

    public static ReplicatorManager getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * binding node implementation
     *
     * @param node
     */
    public void bindNode(Node node) {
        this.node = node;
    }

    public void registerReplicator(Endpoint endpoint, Client client) {
        // expose client instance for another thread (maybe)
        Replicator replicator = new Replicator(endpoint, client, ReplicatorState.CONNECTED);
        replicator.startHeartbeatTimer();
        registerReplicator(replicator);
    }

    public void registerReplicator(Replicator replicator) {
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

    public Map<String, ChannelProcessor> getRegistry() {
        return ImmutableMap.copyOf(registry);
    }

    public ChannelProcessor getResponseProcessor(String clazzName) {
        return getRegistry().get(clazzName);
    }


    @Override
    public void onClazzScanned(Class<?> clazz) {
        Processor kvaftProcessor = clazz.getAnnotation(Processor.class);
        if (Objects.nonNull(kvaftProcessor)
                && kvaftProcessor.handleType().equals(ProcessorType.RESP)) {
            Object instance;
            try {
                instance = clazz.newInstance();
            } catch (Exception e) {
                log.error("replicator channel processor create instance failed", e);
                return;
            }
            if (instance instanceof ChannelProcessor) {
                registry.putIfAbsent(kvaftProcessor.messageClazz().getName(), (ChannelProcessor) instance);
            }
        }
    }

    @Override
    public String scanPackage() {
        return PACKAGE_SCAN;
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
            pipeline.addLast("codec", new KvaftDefaultCodecHandler());
            pipeline.addLast("responseHandler", new ResponseHandler(getInstance()));
            pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
        }
    }
}
