package io.zealab.kvaft.rpc;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;

/**
 * @author LeonWong
 */
@ChannelHandler.Sharable
public class ServerRequestHandler extends ChannelInboundHandlerAdapter {

    private final Map<String, ChannelProcessor<?>> processors;

    public ServerRequestHandler(Map<String, ChannelProcessor<?>> processors) {
        this.processors = processors;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        // TODO
    }
}
