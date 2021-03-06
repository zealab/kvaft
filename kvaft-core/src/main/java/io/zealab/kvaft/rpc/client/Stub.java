package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

import java.util.concurrent.Future;

public interface Stub {

    /**
     * Heartbeat call
     *
     * @param endpoint toWhere
     * @param term     currTerm
     */
    Future<RemoteCalls.HeartbeatAck> heartbeat(Endpoint endpoint, long term);

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
     *
     * @return
     */
    Future<RemoteCalls.AcquireLeaderResp> acquireLeader(Endpoint endpoint);

    /**
     * starting election
     *
     * @param endpoint
     * @param term
     *
     * @return
     */
    Future<RemoteCalls.ElectResp> elect(Endpoint endpoint, long term);

    /**
     * Leader broadcasts step-down message after resetting leader operation
     *
     * @param endpoint
     * @param term
     */
    void stepDown(Endpoint endpoint, long term);
}
