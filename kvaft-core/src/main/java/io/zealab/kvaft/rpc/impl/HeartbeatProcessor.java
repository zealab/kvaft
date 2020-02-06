package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * process heartbeats from client
 *
 * @author LeonWong
 */
@Processor(messageClazz = RemoteCalls.Heartbeat.class)
public class HeartbeatProcessor extends RequestProcessor<RemoteCalls.Heartbeat> {

    @Override
    protected void doProcess0(Peer peer, long requestId, RemoteCalls.Heartbeat payload) {
        cpm.getNode().ifPresent(
                node -> node.handleHeartbeat(peer, requestId, payload.getTerm())
        );
    }
}
