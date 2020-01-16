package io.zealab.kvaft.rpc.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.util.IpAddressUtil;
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

    /**
     * connect to the endpoint and create a channel
     *
     * @param host host
     * @param port port
     *
     * @return is successful?
     *
     * @throws InterruptedException
     */
    public boolean connect(String host, int port) throws InterruptedException {
        ChannelFuture future = bootstrap.connect(host, port).sync();
        if (future.isSuccess()) {
            log.info("connect to host={},port={} successfully.", host, port);
            Channel channel = future.channel();
            String address = IpAddressUtil.convertChannelRemoteAddress(channel);
            String[] array = address.split(",");
            Endpoint endpoint = Endpoint.builder().ip(array[0]).port(Integer.parseInt(array[1])).build();
            Replicator replicator = Replicator.builder().channel(future.channel()).endpoint(endpoint).build();
            rm.addActiveReplicators(replicator);
            return true;
        } else {
            log.warn("connect to host={},port={} failed.", host, port);
            log.error("connection establish failed", future.cause());
            return false;
        }
    }
}
