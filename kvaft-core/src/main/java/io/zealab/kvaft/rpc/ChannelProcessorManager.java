package io.zealab.kvaft.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelFuture;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Node;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.Scanner;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    private final Map<String, ChannelProcessor> registry = Maps.newHashMap();

    private final Map<String, Peer> peers = Maps.newHashMap();

    /**
     * channel process package directory
     */
    private final static String PACKAGE_SCAN = "io.zealab.kvaft";

    private final ReadWriteLock peersLock = new ReentrantReadWriteLock();

    private volatile Node node = null;

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
    public ChannelProcessor getProcessor(String payloadClazz) {
        return registry.get(payloadClazz);
    }

    /**
     * retrieve registry
     *
     * @return registry
     */
    public Map<String, ChannelProcessor> getRegistry() {
        return ImmutableMap.copyOf(registry);
    }

    /**
     * store peer in memory
     *
     * @param peer
     */
    public void addPeer(Peer peer) {
        final Lock wl = peersLock.writeLock();
        wl.lock();
        try {
            peers.put(peer.getEndpoint().toString(), peer);
            log.info("A new peer={} has been added to the channel processor manager", peer.toString());
        } finally {
            wl.unlock();
        }
    }

    /**
     * binding node implementation
     *
     * @param node
     */
    public void bindNode(Node node) {
        this.node = node;
    }

    /**
     * get current node
     *
     * @return
     */
    public Optional<Node> getNode() {
        return Optional.ofNullable(node);
    }

    /**
     * retrieve peer by node id
     *
     * @param nodeId node
     *
     * @return
     */
    public Peer getPeer(String nodeId) {
        final Lock rl = peersLock.readLock();
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
        final Lock wl = peersLock.writeLock();
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

    public void clearAllPeers() {
        final Lock wl = peersLock.writeLock();
        try {
            wl.lock();
            peers.entrySet().parallelStream().forEach(
                    entry -> {
                        Peer p = entry.getValue();
                        log.info("peer={} will be closed soon..", p.toString());
                        p.close();
                    }
            );
            peers.clear();
        } finally {
            wl.unlock();
        }
    }

    public int peerSize() {
        final Lock rl = peersLock.readLock();
        try {
            rl.lock();
            return peers.size();
        } finally {
            rl.unlock();
        }
    }


    /**
     * handle timeout channel validation
     *
     * @param timeoutMs timeout
     */
    public void handleSessionTimeoutPeers(int timeoutMs) {
        final long currTime = System.currentTimeMillis();
        final Lock rl = peersLock.readLock();
        Map<String, Peer> copy;
        try {
            rl.lock();
            copy = ImmutableMap.copyOf(peers);
        } finally {
            rl.unlock();
        }
        Lock wl = peersLock.writeLock();
        try {
            wl.lock();
            copy.entrySet().parallelStream().forEach(
                    entry -> {
                        Peer peer = entry.getValue();
                        if (!peer.getChannel().isActive()
                                || currTime - peer.getLastHbTime() > timeoutMs) {
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
                registry.putIfAbsent(kvaftProcessor.messageClazz().getName(), (ChannelProcessor) instance);
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
