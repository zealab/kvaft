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
    Future<RemoteCalls.PreVoteAck> preVote(Endpoint endpoint, long term);

    /**
     * acquire leader information
     *
     * @param endpoint
     * @return
     */
    Future<RemoteCalls.AcquireLeaderResp> acquireLeader(Endpoint endpoint);
}
