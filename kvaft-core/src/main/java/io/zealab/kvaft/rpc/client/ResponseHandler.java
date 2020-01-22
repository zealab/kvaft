package io.zealab.kvaft.rpc.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * response handler
 */
@Slf4j
@ChannelHandler.Sharable
public class ResponseHandler extends ChannelInboundHandlerAdapter {

    private final ReplicatorManager rm;

    public ResponseHandler(ReplicatorManager rm) {
        this.rm = rm;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("ResponseHandler received a message={}", msg.toString());
        if (msg instanceof KvaftMessage<?>) {
            KvaftMessage<?> kvaftMessage = (KvaftMessage<?>) msg;
            Class<?> clazz = kvaftMessage.payload().getClass();
            Optional.ofNullable(rm.getResponseProcessor(clazz.getName()))
                    .ifPresent(
                            channelProcessor -> channelProcessor.doProcess(kvaftMessage, ctx.channel())
                    );
        }
        super.channelRead(ctx, msg);
    }
}
