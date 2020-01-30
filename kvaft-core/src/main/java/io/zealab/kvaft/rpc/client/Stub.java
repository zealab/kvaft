package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

import java.util.concurrent.Future;

public interface Stub {

    /**
     * Heartbeat call
     *
     * @param endpoint toWhere
     */
    void heartbeat(Endpoint endpoint);

    /**
     * pre vote req
     *
     * @param endpoint toWhere
     * @param term     currTerm
     */
    Future<RemoteCalls.PreVoteAck> preVoteReq(Endpoint endpoint, long term);
}
