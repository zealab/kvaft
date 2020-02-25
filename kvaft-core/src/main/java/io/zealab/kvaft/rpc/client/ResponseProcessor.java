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
        Replicator replicator = rm.getReplicator(peer);
        Assert.notNull(replicator, String.format("The remote peer=%s is not available.", peer.getEndpoint().toString()));
        Map<Long, Callback> context = replicator.getClient().getClientContext();
        Optional.ofNullable(
                context.get(msg.requestId())
        ).ifPresent(
                callback -> {
                    context.remove(msg.requestId());
                    callback.apply(payload);
                }
        );
    }
}
