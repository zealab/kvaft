package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * @author LeonWong
 */
@Processor(messageClazz = RemoteCalls.ElectReq.class)
public class ElectingProcessor extends RequestProcessor<RemoteCalls.ElectReq> {

    @Override
    protected void doProcess0(Peer peer, long requestId, RemoteCalls.ElectReq payload) {
        cpm.getNode().ifPresent(
                node -> node.handleElectRequest(peer, requestId, payload.getTerm())
        );
    }
}
