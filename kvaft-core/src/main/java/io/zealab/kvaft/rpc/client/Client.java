package io.zealab.kvaft.rpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.TimeoutException;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.core.ReplicatorState;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.codec.KvaftDefaultCodecHandler;
import io.zealab.kvaft.util.Assert;
import io.zealab.kvaft.util.IpAddressUtil;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.zealab.kvaft.util.NettyUtil.getClientSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

@Slf4j
public class Client implements Initializer {

    private Bootstrap bootstrap = new Bootstrap();

    private final ReplicatorManager rm = ReplicatorManager.getInstance();

    private volatile SimpleChannelPool channelPool;

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "client-worker-group-%d");

    private final Endpoint endpoint;

    public Client(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void init() {
        bootstrap.remoteAddress(endpoint.getIp(), endpoint.getPort());
        bootstrap.group(workerGroup)
                .channel(getClientSocketChannelClass())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast("codec", new KvaftDefaultCodecHandler());
                                pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
                            }
                        }
                );
        channelPool = new SimpleChannelPool(bootstrap, new ReplicatorManager.ReplicatorChannelPoolHandler(), ChannelHealthChecker.ACTIVE);
    }

    /**
     * connect to the endpoint and create a channel
     *
     * @return channel
     *
     * @throws InterruptedException
     */
    @Nullable
    public Channel connect(int cTimeout) throws InterruptedException, ExecutionException {
        ensureInitialize();
        Future<Channel> future = channelPool.acquire();
        long beginTime = System.currentTimeMillis();
        while (!future.isDone() && System.currentTimeMillis() - beginTime <= cTimeout) {
            Thread.sleep(1000L);
        }
        if (!future.isDone()) {
            log.error("connection establish timeout in {} ms, endpoint is {}:{}", cTimeout, endpoint.getIp(), endpoint.getPort());
            return null;
        }
        Channel channel = future.get();
        log.info("connect to host={},port={} successfully.", endpoint.getIp(), endpoint.getPort());
        String address = IpAddressUtil.convertChannelRemoteAddress(channel);
        String[] array = address.split(":");
        Endpoint endpoint = Endpoint.builder().ip(array[0]).port(Integer.parseInt(array[1])).build();
        // expose client instance for another thread (maybe)
        Replicator replicator = new Replicator(endpoint, this, ReplicatorState.CONNECTED);
        replicator.startHeartbeatTimer();
        rm.registerActiveReplicator(replicator);
        return channel;
    }

    /**
     * retrieve client channel
     *
     * @return the channel
     */
    @Nullable
    public Future<Channel> getConnection() {
        ensureInitialize();
        return channelPool.acquire();
    }

    /**
     * get connection from pool
     *
     * @param cTimeout
     *
     * @return
     */
    @Nullable
    public Channel getConnection(int cTimeout) {
        ensureInitialize();
        final Future<Channel> channelFuture = channelPool.acquire();
        Assert.notNull(channelFuture, "channel future could not be null");
        try {
            return channelFuture.get(cTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.error("rpc connection timeout in " + cTimeout + " ms", e);
            return null;
        } catch (Exception e) {
            log.error("rpc connection establish failed.", e);
            return null;
        }
    }

    private void ensureInitialize() {
        if (Objects.isNull(channelPool)) {
            throw new IllegalStateException("Client is not finish initialization yet.");
        }
    }

    /**
     * invoke in one way method
     *
     * @param channel   connection
     * @param req       req entity
     * @param soTimeout socket timeout in milliseconds
     */
    public void invokeOneWay(Channel channel, KvaftMessage<?> req, int soTimeout) {
        long begin = System.currentTimeMillis();
        // tips: channel.writeAndFlush is thread-safe
        try {
            channel.writeAndFlush(req).addListener(
                    (ChannelFutureListener) future -> {
                        while (!future.isDone() && System.currentTimeMillis() - begin <= soTimeout) {
                            Thread.sleep(1000);
                        }
                        if (!future.isDone()) {
                            log.error("rpc request socket timeout in {} ms.", soTimeout);
                            return;
                        }
                        if (!future.isSuccess()) {
                            log.error("rpc request failed.", future.cause());

                        }
                        log.info("rpc request successfully.");
                    }
            );
        } finally {
            channelPool.release(channel);
        }
    }

    /**
     * shutdown client
     */
    public void close() {
        channelPool.close();
        workerGroup.shutdownGracefully();
    }
}
