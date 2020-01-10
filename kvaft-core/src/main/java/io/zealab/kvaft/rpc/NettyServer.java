package io.zealab.kvaft.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.protoc.KvaftDefaultSerializerHandler;

import java.util.Map;

import static io.netty.channel.ChannelOption.SO_BACKLOG;
import static io.netty.channel.ChannelOption.WRITE_SPIN_COUNT;
import static io.zealab.kvaft.util.NettyUtil.getServerSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

/**
 * @author LeonWong
 */
public class NettyServer implements Initializer {

    private final EventLoopGroup bossGroup = newEventLoopGroup(1, "boss-loop-group-%d");

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "worker-loop-group-%d");

    private final int port;

    public NettyServer(int port) {
        this.port = port;
    }


    @Override
    public void init() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(getServerSocketChannelClass())
                .option(SO_BACKLOG, 1024)
                .option(WRITE_SPIN_COUNT, 10);
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        Map<String, ChannelProcessor<?>> processors = ChannelProcessorManager.getRegistry();
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
                        pipeline.addLast("serializer", new KvaftDefaultSerializerHandler());
                        pipeline.addLast("serverRequestHandler", new ServerRequestHandler(processors));
                    }
                }
        );

    }
}
