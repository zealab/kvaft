package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * handle preVote stage message
 *
 * @author LeonWong
 */
@Processor(messageClazz = RemoteCalls.PreVoteReq.class)
public class PreVoteProcessor extends RequestProcessor<RemoteCalls.PreVoteReq> {

    @Override
    protected void doProcess0(Peer peer, long requestId, RemoteCalls.PreVoteReq payload) {
        cpm.getNode().ifPresent(
                node -> node.handlePreVoteRequest(peer, requestId, payload.getTerm())
        );
    }
}
