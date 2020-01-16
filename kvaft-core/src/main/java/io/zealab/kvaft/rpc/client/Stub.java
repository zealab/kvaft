package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

public interface Stub {

    void heartbeat(Replicator replicator);

    RemoteCalls.PreVoteAck preVoteReq(Endpoint endpoint, long term);
}
