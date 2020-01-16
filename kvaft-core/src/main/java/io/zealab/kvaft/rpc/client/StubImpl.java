package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Replicator;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

public class StubImpl implements Stub {


    @Override
    public void heartbeat(Replicator replicator) {

    }

    @Override
    public RemoteCalls.PreVoteAck preVoteReq(Endpoint endpoint, long term) {
        RemoteCalls.PreVoteReq preVoteReq = RemoteCalls.PreVoteReq.newBuilder().setTerm(term).build();
        KvaftMessage<RemoteCalls.PreVoteReq> req = KvaftMessage.<RemoteCalls.PreVoteReq>builder()
                .payload(preVoteReq)
                .requestId(System.currentTimeMillis())
                .build();
        return null;
    }
}
