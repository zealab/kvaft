package io.zealab.kvaft.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelFuture;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.Scanner;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * channel processors manager
 *
 * @author LeonWong
 */
@Slf4j
public class ChannelProcessorManager implements Scanner {

    private final Map<String, ChannelProcessor<?>> registry = Maps.newHashMap();

    private final Map<String, Peer> peers = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io.zealab.kvaft";

    private final ReadWriteLock peerLock = new ReentrantReadWriteLock();

    private ChannelProcessorManager() {}

    public static ChannelProcessorManager getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * get payload processor
     *
     * @param payloadClazz clazz
     *
     * @return the channel processor
     */
    public ChannelProcessor<?> getProcessor(String payloadClazz) {
        return registry.get(payloadClazz);
    }

    /**
     * retrieve registry
     *
     * @return registry
     */
    public Map<String, ChannelProcessor<?>> getRegistry() {
        return ImmutableMap.copyOf(registry);
    }

    /**
     * store peer in memory
     *
     * @param peer
     */
    public void addPeer(Peer peer) {
        final Lock wl = peerLock.writeLock();
        wl.lock();
        try {
            peers.put(peer.getEndpoint().toString(), peer);
            log.info("A new peer={} has been added to the channel processor manager", peer.toString());
        } finally {
            wl.unlock();
        }
    }

    /**
     * retrieve peer by node id
     *
     * @param nodeId node
     *
     * @return
     */
    public Peer getPeer(String nodeId) {
        final Lock rl = peerLock.readLock();
        rl.lock();
        try {
            return peers.get(nodeId);
        } finally {
            rl.unlock();
        }
    }

    /**
     * remove peer
     *
     * @param nodeId
     */
    public void removePeer(String nodeId) {
        final Lock wl = peerLock.writeLock();
        wl.lock();
        try {
            Peer peer = peers.remove(nodeId);
            if (peer != null) {
                log.info("The peer={} has been removed from the channel processor manager", peer.toString());
            }
        } finally {
            wl.unlock();
        }
    }

    /**
     * handle timeout channel validation
     *
     * @param timeoutInMs timeout
     */
    public void handleTimeoutPeers(int timeoutInMs) {
        final long currTime = System.currentTimeMillis();
        final Lock rl = peerLock.readLock();
        rl.lock();
        Map<String, Peer> copy;
        try {
            copy = ImmutableMap.copyOf(peers);
        } finally {
            rl.unlock();
        }
        Lock wl = peerLock.writeLock();
        try {
            copy.entrySet().parallelStream().forEach(
                    entry -> {
                        Peer peer = entry.getValue();
                        if (!peer.getChannel().isActive()
                                || currTime - peer.getLastHbTime() > timeoutInMs) {
                            peers.remove(peer.getEndpoint().toString());
                            ChannelFuture future = peer.getChannel().close();
                            if (future.isSuccess()) {
                                log.warn("The node={},channel={} does not report heartbeat anymore", peer.getEndpoint().toString(), peer.getEndpoint().toString());
                            }
                        }
                    }
            );
        } finally {
            wl.unlock();
        }
    }

    @Override
    public void onClazzScanned(Class<?> clazz) {
        Processor kvaftProcessor = clazz.getAnnotation(Processor.class);
        if (Objects.nonNull(kvaftProcessor)) {
            Object instance;
            try {
                instance = clazz.newInstance();
            } catch (Exception e) {
                log.error("channel processor create instance failed", e);
                return;
            }
            if (instance instanceof ChannelProcessor) {
                registry.putIfAbsent(kvaftProcessor.messageClazz().toString(), (ChannelProcessor<?>) instance);
            }
        }
    }

    @Override
    public String scanPackage() {
        return PACKAGE_SCAN;
    }

    private static class SingletonHolder {
        public static ChannelProcessorManager instance = new ChannelProcessorManager();
    }
}
