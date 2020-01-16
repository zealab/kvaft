package io.zealab.kvaft.rpc.impl;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.ChannelProcessor;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;

/**
 * server side's abstract processor
 *
 * @author LeonWong
 */
@Slf4j
public abstract class AbstractProcessor<T extends Message> implements ChannelProcessor<T> {

    protected final static ChannelProcessorManager cpm = ChannelProcessorManager.getInstance();

    /**
     * handle payload
     *
     * @param payload
     */
    protected abstract void doProcess0(Peer peer, T payload);

    /**
     * pre process
     *
     * @param msg message entity
     */
    public void doProcess(KvaftMessage<T> msg, Channel channel) {
        checkMsg(msg);
        Peer peer = Peer.from(channel);
        Assert.notNull(peer, "peer could not be null");
        doProcess0(peer, msg.payload());
    }

    private void checkMsg(KvaftMessage<T> msg) {
        // TODO verify request id & nodeid
    }
}
