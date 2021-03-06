package io.zealab.kvaft.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.protoc.codec.KvaftDefaultCodecHandler;
import io.zealab.kvaft.util.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.netty.channel.ChannelOption.SO_BACKLOG;
import static io.netty.channel.ChannelOption.WRITE_SPIN_COUNT;
import static io.zealab.kvaft.util.NettyUtil.getServerSocketChannelClass;
import static io.zealab.kvaft.util.NettyUtil.newEventLoopGroup;

/**
 * @author LeonWong
 */
@Slf4j
public class NioServer implements Initializer {

    private final ExecutorService executorService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("netty-boot-thread-%d", true));

    private final EventLoopGroup bossGroup = newEventLoopGroup(1, "boss-loop-group-%d");

    private final EventLoopGroup workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, "worker-loop-group-%d");

    private final String host;

    private final int port;

    private final ServerBootstrap bootstrap = new ServerBootstrap();

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    public NioServer(String host, int port) {
        this.host = host;
        this.port = port;
    }


    @Override
    public void init() {
        bootstrap.group(bossGroup, workerGroup)
                .channel(getServerSocketChannelClass())
                .option(SO_BACKLOG, 1024)
                .option(WRITE_SPIN_COUNT, 10)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast("flushConsolidationHandler", new FlushConsolidationHandler(1024, true));
                                pipeline.addLast("codec", new KvaftDefaultCodecHandler());
                                pipeline.addLast("connectionHandler", new ConnectionHandler(processManager));
                                pipeline.addLast("serverRequestHandler", new ServerRequestHandler(processManager));
                            }
                        }
                );
    }

    /**
     * do start netty server
     */
    public void start() {
        executorService.execute(
                () -> {
                    log.info("netty server starting...");
                    ChannelFuture future = bootstrap.bind(port);
                    while (!future.isDone()) {
                        // ignore
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error("starting netty server was interrupted");
                        }
                    }
                    if (future.isSuccess()) {
                        log.info("netty server started host={},port={}", host, port);
                    } else {
                        log.error(String.format("failed to bind address host=%s,port=%d", host, port), future.cause());
                        System.exit(1);
                    }
                }
        );
    }
}
