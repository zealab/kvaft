package io.zealab.kvaft.rpc.client;

import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.Callback;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;

import static io.zealab.kvaft.util.NettyUtil.getClientSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

@Slf4j
public class Client implements Initializer {

    private volatile SimpleChannelPool channelPool;

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "client-worker-group-%d");

    private final Endpoint endpoint;

    private final Map<Long, Callback> context = Maps.newConcurrentMap();

    public Client(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void init() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.remoteAddress(endpoint.getIp(), endpoint.getPort())
                .group(workerGroup)
                .channel(getClientSocketChannelClass())
                .option(ChannelOption.SO_KEEPALIVE, true);
        channelPool = new FixedChannelPool(bootstrap, new ReplicatorManager.ReplicatorChannelPoolHandler(), 10, 1024);
    }

    /**
     * invoke in one way method
     *
     * @param req       request entity
     * @param cTimeout  connection timeout
     * @param soTimeout socket timeout in milliseconds
     */
    public void invokeOneWay(KvaftMessage<?> req, int cTimeout, int soTimeout) {
        ensureInitialize();
        Optional.ofNullable(
                createChannel(cTimeout)
        ).ifPresent(
                channel -> {
                    long begin = System.currentTimeMillis();
                    try {
                        // tips: channel.writeAndFlush is thread-safe
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
        );
    }

    /**
     * invoke with promise
     *
     * @param req      request
     * @param cTimeout connection timeout
     * @param callback  future listener
     */
    public void invokeWithCallback(KvaftMessage<?> req, int cTimeout, int soTimeout, Callback callback) {
        ensureInitialize();
        Optional.ofNullable(
                createChannel(cTimeout)
        ).ifPresent(
                channel -> {
                    final long begin = System.currentTimeMillis();
                    try {
                        log.info("sending request to channel={}", channel.toString());
                        // put callback function into context
                        context.put(req.requestId(), callback);
                        // tips: channel.writeAndFlush is thread-safe
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
        );
    }

    /**
     * get current context
     *
     * @return
     */
    public Map<Long, Callback> getClientContext() {
        return context;
    }

    /**
     * get connection from pool
     *
     * @param cTimeout connection timeout
     *
     * @return
     */
    @Nullable
    private Channel createChannel(int cTimeout) {
        ensureInitialize();
        final Future<Channel> channelFuture = channelPool.acquire();
        try {
            return channelFuture.get(cTimeout, TimeUnit.MILLISECONDS);
        } catch (CancellationException e) {
            log.error("rpc connection was cancelled.");
        } catch (InterruptedException e) {
            log.error("rpc connection was interrupted.");
        } catch (ExecutionException e) {
            log.error("rpc connection execution has an error ");
        } catch (TimeoutException e) {
            log.error(String.format("rpc connection timeout in %d ms", cTimeout));
        }
        return null;
    }


    private void ensureInitialize() {
        if (Objects.isNull(channelPool)) {
            throw new IllegalStateException("Client is not finish initialization yet.");
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
