package io.zealab.kvaft.rpc.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * response handler
 */
@Slf4j
@ChannelHandler.Sharable
public class ResponseHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("ResponseHandler received a message={}", msg.toString());
        if (msg instanceof KvaftMessage<?>) {
            ResponseProcessor responseProcessor = new ResponseProcessor();
            KvaftMessage<?> kvaftMessage = (KvaftMessage<?>) msg;
            responseProcessor.doProcess(kvaftMessage, ctx.channel());
        }
        super.channelRead(ctx, msg);
    }
}
