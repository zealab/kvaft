package io.zealab.kvaft.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;

/**
 * handle connection events
 *
 * @author LeonWong
 */
@Slf4j
public class ConnectionHandler extends ChannelInboundHandlerAdapter {

    private final ChannelProcessorManager cpm;

    public ConnectionHandler(ChannelProcessorManager cpm) {
        this.cpm = cpm;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Peer peer = Peer.from(ctx.channel());
        Assert.notNull(peer, "peer could not be null");
        cpm.addPeer(peer);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Peer peer = Peer.from(ctx.channel());
        Assert.notNull(peer, "peer could not be null");
        cpm.removePeer(peer.nodeId());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Peer peer = Peer.from(ctx.channel());
        Assert.notNull(peer, "peer could not be null");
        log.error("exception caught ", cause);
    }
}
