package io.zealab.kvaft.rpc.impl;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.config.Processor;
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
public abstract class AbstractProcessor<T extends Message> implements ChannelProcessor {

    protected final static ChannelProcessorManager cpm = ChannelProcessorManager.getInstance();

    /**
     * handle payload
     *
     * @param payload
     */
    protected abstract void doProcess0(Peer peer, T payload);

    /**
     * assert type match
     *
     * @param payload payload
     */
    @Override
    public void assertMatch(Message payload) {
        Processor processor = this.getClass().getAnnotation(Processor.class);
        if (null != processor) {
            boolean isMatch = processor.messageClazz().getName().equals(payload.getClass().getName());
            if (!isMatch) {
                throw new IllegalArgumentException("payload is not match here");
            }
        }
    }

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
        doProcess0(peer, (T) payload);
    }
}
