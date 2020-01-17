package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;

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
    void preVoteReq(Endpoint endpoint, long term);
}
