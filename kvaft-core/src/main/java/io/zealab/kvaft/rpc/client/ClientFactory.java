package io.zealab.kvaft.rpc.client;

import io.netty.channel.Channel;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Replicator;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * @author LeonWong
 */
@Slf4j
@ThreadSafe
public class ClientFactory {

    private final static ReplicatorManager replicatorManager = ReplicatorManager.getInstance();

    /**
     * client creator with double check
     *
     * @param endpoint toWhere
     *
     * @return
     */
    @Nullable
    public static Client getOrCreate(@NonNull Endpoint endpoint) {
        try {
            final String nodeId = endpoint.toString();
            Replicator replicator = replicatorManager.getReplicator(nodeId);
            if (null != replicator) {
                return replicator.getClient();
            }
            synchronized (nodeId.intern()) {
                replicator = replicatorManager.getReplicator(nodeId);
                if (null != replicator) {
                    return replicator.getClient();
                }
                Client client = new Client(endpoint);
                client.init();
                Channel channel = client.connect(5000);
                return null != channel ? client : null;
            }
        } catch (Exception e) {
            log.error("create client failed, please check endpoint config", e);
            return null;
        }
    }
}
