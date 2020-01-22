package io.zealab.kvaft.rpc.client;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.rpc.impl.AbstractProcessor;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.util.Assert;

/**
 * @param <T>
 *
 * @author LeonWong
 */
public abstract class ResponseProcessor<T extends Message> extends AbstractProcessor {

    protected final static ReplicatorManager rm = ReplicatorManager.getInstance();

    /**
     * handle payload, it's a extension point for developer create a new processor
     *
     * @param payload payload
     */
    protected abstract void doProcess0(Client client, T payload);

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
        Replicator replicator = rm.getReplicator(peer.getEndpoint().toString());
        Assert.notNull(replicator, "The replicator is not existed");
        doProcess0(replicator.getClient(), (T) payload);
    }
}
