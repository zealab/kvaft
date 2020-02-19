package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

@Processor(messageClazz = RemoteCalls.AcquireLeaderReq.class)
public class AcquireLeaderProcessor extends RequestProcessor<RemoteCalls.AcquireLeaderReq> {

    @Override
    protected void doProcess0(Peer peer, long requestId, RemoteCalls.AcquireLeaderReq payload) {
        cpm.getNode().ifPresent(
                node -> node.handleLeaderAcquire(peer, requestId)
        );
    }
}
