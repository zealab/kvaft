package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * @author LeonWong
 */
@Processor(messageClazz = RemoteCalls.PreVoteReq.class)
public class PreVoteProcessor extends AbstractProcessor<RemoteCalls.PreVoteReq> {

    @Override
    protected void doProcess0(Peer peer, RemoteCalls.PreVoteReq payload) {
        // TODO
    }
}
