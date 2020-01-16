package io.zealab.kvaft.rpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.zealab.kvaft.core.Initializer;
import lombok.extern.slf4j.Slf4j;

import static io.zealab.kvaft.util.NettyUtil.getClientSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

@Slf4j
public class NettyClient implements Initializer {

    private final Bootstrap bootstrap = new Bootstrap();

    private final ReplicatorManager rm = ReplicatorManager.getInstance();

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "client-worker-group-%d");

    private final String host;

    private final int port;

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void init() {
        bootstrap.group(workerGroup)
                .channel(getClientSocketChannelClass())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                // TODO
                            }
                        }
                );


    }

    public boolean connect(String host, int port) throws InterruptedException {
        ChannelFuture future = bootstrap.connect(host, port).sync();
        if (future.isSuccess()) {
            log.info("connect to host={},port={} successfully.", host, port);
            rm.addActiveReplicators();
            return true;
        } else {
            log.warn("connect to host={},port={} failed.", host, port);
            log.error("connection establish failed", future.cause());
            return false;
        }
    }
}
