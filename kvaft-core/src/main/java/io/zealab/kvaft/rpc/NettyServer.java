package io.zealab.kvaft.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.protoc.codec.KvaftDefaultCodec;
import io.zealab.kvaft.util.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.netty.channel.ChannelOption.SO_BACKLOG;
import static io.netty.channel.ChannelOption.WRITE_SPIN_COUNT;
import static io.zealab.kvaft.util.NettyUtil.getServerSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

/**
 * @author LeonWong
 */
@Slf4j
public class NettyServer implements Initializer {

    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("netty-boot-thread-%d", true));

    private final EventLoopGroup bossGroup = newEventLoopGroup(1, "boss-loop-group-%d");

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "worker-loop-group-%d");

    private final int port;

    private final ServerBootstrap bootstrap = new ServerBootstrap();

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    public NettyServer(int port) {
        this.port = port;
    }

    @Override
    public void init() {
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
                        Map<String, ChannelProcessor<?>> processors = processManager.getRegistry();
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
                        pipeline.addLast("codec", new KvaftDefaultCodec());
                        pipeline.addLast("serverRequestHandler", new ServerRequestHandler(processors));
                    }
                }
        );
    }

    /**
     * do start netty server
     *
     * @throws InterruptedException
     */
    public void start() throws InterruptedException {
        executorService.execute(
                () -> {
                    try {
                        log.info("netty server starting...");
                        ChannelFuture future = bootstrap.bind(port).sync();
                        if (future.isSuccess()) {
                            log.info("netty server started ip={},port={}", "127.0.0.1", port);
                        }
                    } catch (InterruptedException e) {
                        log.error("do start netty server...");
                    }
                }
        );

    }
}
