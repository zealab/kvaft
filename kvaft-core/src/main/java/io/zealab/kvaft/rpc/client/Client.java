package io.zealab.kvaft.rpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.*;

import static io.zealab.kvaft.util.NettyUtil.getClientSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

@Slf4j
public class Client implements Initializer {

    private volatile SimpleChannelPool channelPool;

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "client-worker-group-%d");

    private final Endpoint endpoint;

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
     * @param req       req entity
     * @param soTimeout socket timeout in milliseconds
     */
    public void invokeOneWay(KvaftMessage<?> req, int cTimeout, int soTimeout) {
        ensureInitialize();
        final Channel channel = lazyGet(cTimeout);
        Assert.notNull(channel, "channel future could not be null");
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

    /**
     * get connection from pool
     *
     * @param cTimeout connection timeout
     *
     * @return
     */
    @Nullable
    private Channel lazyGet(int cTimeout) {
        ensureInitialize();
        final Future<Channel> channelFuture = channelPool.acquire();
        Assert.notNull(channelFuture, "channel future could not be null");
        try {
            return channelFuture.get(cTimeout, TimeUnit.MILLISECONDS);
        } catch (CancellationException e) {
            log.error("rpc connection was cancelled.", e);
        } catch (InterruptedException e) {
            log.error("rpc connection was interrupted.", e);
        } catch (ExecutionException e) {
            log.error("rpc connection execution has an error ", e);
        } catch (TimeoutException e) {
            log.error(String.format("rpc connection timeout in %d ms", cTimeout), e);
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
