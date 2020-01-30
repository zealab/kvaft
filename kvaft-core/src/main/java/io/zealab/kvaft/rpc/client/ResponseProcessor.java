package io.zealab.kvaft.rpc.client;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.rpc.Callback;
import io.zealab.kvaft.rpc.impl.AbstractProcessor;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.util.Assert;

import java.util.Map;
import java.util.Optional;

/**
 * unified rpc response processor
 *
 * @author LeonWong
 */
public class ResponseProcessor extends AbstractProcessor {

    protected final static ReplicatorManager rm = ReplicatorManager.getInstance();

    /**
     * response process
     *
     * @param msg message entity
     */
    @Override
    public void doProcess(KvaftMessage<?> msg, Channel channel) {
        Message payload = msg.payload();
        assertMatch(payload);
        Peer peer = Peer.from(channel);
        Assert.notNull(peer, "peer could not be null");
        // replace it
        Replicator replicator = rm.getReplicator(peer.getEndpoint().toString());
        Assert.notNull(replicator, "The replicator is not existed");
        Map<Long, Callback> context = replicator.getClient().getClientContext();
        Optional.ofNullable(
                context.get(msg.requestId())
        ).ifPresent(
                callback -> callback.apply(payload)
        );
    }
}
