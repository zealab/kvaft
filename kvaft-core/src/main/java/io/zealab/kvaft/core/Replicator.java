package io.zealab.kvaft.core;

import io.netty.channel.Channel;
import lombok.Builder;
import lombok.ToString;

/**
 * replicator
 *
 * @author LeonWong
 */
@Builder
@ToString
public class Replicator {

    private Endpoint endpoint;

    private Channel channel;

    public String nodeId() {
        return endpoint.toString();
    }
}
