package io.zealab.kvaft.rpc.impl;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.util.Assert;

/**
 * @author LeonWong
 */
public abstract class RequestProcessor<T extends Message> extends AbstractProcessor {

    protected final static ChannelProcessorManager cpm = ChannelProcessorManager.getInstance();

    /**
     * handle payload, it's a extension point for developer create a new processor
     *
     * @param peer       peer
     * @param requestId  requestId
     * @param payload    payload
     */
    protected abstract void doProcess0(Peer peer, long requestId, T payload);

    /**
     * pre process
     *
     * @param msg message entity
     */
    @Override
    @SuppressWarnings("unchecked")
    public void doProcess(KvaftMessage<?> msg, Channel channel) {
        Message payload = msg.payload();
        assertMatch(payload);
        Peer peer = Peer.from(channel);
        Assert.notNull(peer, "peer could not be null");
        // replace it
        peer = cpm.getPeer(peer.getEndpoint().toString());
        doProcess0(peer, msg.requestId(), (T) payload);
    }
}
