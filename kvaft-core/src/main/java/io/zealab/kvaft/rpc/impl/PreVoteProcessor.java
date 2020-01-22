package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.ProcessorType;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * handle preVote stage message
 *
 * @author LeonWong
 */
@Processor(handleType = ProcessorType.REQ, messageClazz = RemoteCalls.PreVoteReq.class)
public class PreVoteProcessor extends RequestProcessor<RemoteCalls.PreVoteReq> {

    @Override
    protected void doProcess0(Peer peer, RemoteCalls.PreVoteReq payload) {
        cpm.getNode().ifPresent(
                node -> node.handlePreVoteRequest(peer, payload.getTerm())
        );
    }
}
