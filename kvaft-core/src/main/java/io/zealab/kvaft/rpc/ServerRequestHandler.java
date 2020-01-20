package io.zealab.kvaft.rpc;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * @author LeonWong
 */
@Slf4j
@ChannelHandler.Sharable
public class ServerRequestHandler extends ChannelInboundHandlerAdapter {

    private final ChannelProcessorManager cpm;

    public ServerRequestHandler(ChannelProcessorManager cpm) {
        this.cpm = cpm;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("ServerRequestHandler received a message={}", msg.toString());
        if (msg instanceof KvaftMessage<?>) {
            KvaftMessage<?> kvaftMessage = (KvaftMessage<?>) msg;
            Class<?> clazz = kvaftMessage.payload().getClass();
            Optional.ofNullable(cpm.getProcessor(clazz.getName()))
                    .ifPresent(
                            channelProcessor -> channelProcessor.doProcess(kvaftMessage, ctx.channel())
                    );
        }
        super.channelRead(ctx, msg);
    }
}
