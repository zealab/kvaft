package io.zealab.kvaft.rpc;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author LeonWong
 */
@ChannelHandler.Sharable
public class ServerRequestHandler extends ChannelInboundHandlerAdapter {

    private final ChannelProcessorManager cpm;

    public ServerRequestHandler(ChannelProcessorManager cpm) {
        this.cpm = cpm;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        // TODO
    }
}
