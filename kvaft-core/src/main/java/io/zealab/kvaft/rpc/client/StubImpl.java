package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.RequestId;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class StubImpl implements Stub {

    @Override
    public void heartbeat(Endpoint endpoint) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.Heartbeat heartbeat = RemoteCalls.Heartbeat.newBuilder().setTimestamp(requestId.getCreateTime()).build();
        KvaftMessage<RemoteCalls.Heartbeat> req = KvaftMessage.<RemoteCalls.Heartbeat>builder()
                .requestId(requestId.getValue())
                .payload(heartbeat)
                .build();
        client.invokeOneWay(req, 1000, 1000);
    }

    @Override
    public void preVoteReq(Endpoint endpoint, long term) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.PreVoteReq preVoteReq = RemoteCalls.PreVoteReq.newBuilder().setTerm(term).build();
        KvaftMessage<RemoteCalls.PreVoteReq> req = KvaftMessage.<RemoteCalls.PreVoteReq>builder()
                .payload(preVoteReq)
                .requestId(requestId.getValue())
                .build();
        client.invokeOneWay(req, 1000, 1000);
    }
}