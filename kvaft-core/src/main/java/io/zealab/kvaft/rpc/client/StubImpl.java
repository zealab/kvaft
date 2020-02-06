package io.zealab.kvaft.rpc.client;

import com.google.common.util.concurrent.SettableFuture;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.RequestId;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;


@Slf4j
public class StubImpl implements Stub {

    @Override
    public Future<RemoteCalls.HeartbeatAck> heartbeat(Endpoint endpoint) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.Heartbeat heartbeat = RemoteCalls.Heartbeat.newBuilder().setTimestamp(requestId.getCreateTime()).build();
        KvaftMessage<RemoteCalls.Heartbeat> req = KvaftMessage.<RemoteCalls.Heartbeat>builder()
                .requestId(requestId.getValue())
                .payload(heartbeat)
                .build();
        SettableFuture<RemoteCalls.HeartbeatAck> result = SettableFuture.create();
        client.invokeWithCallback(req, 1000, 1000, payload -> {
            log.info("preVote response={}", payload.toString());
            RemoteCalls.HeartbeatAck ack = (RemoteCalls.HeartbeatAck) payload;
            result.set(ack);
        });
        return result;
    }

    @Override
    public Future<RemoteCalls.PreVoteAck> preVote(Endpoint endpoint, long term) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.PreVoteReq preVoteReq = RemoteCalls.PreVoteReq.newBuilder().setTerm(term).build();
        KvaftMessage<RemoteCalls.PreVoteReq> req = KvaftMessage.<RemoteCalls.PreVoteReq>builder()
                .payload(preVoteReq)
                .requestId(requestId.getValue())
                .build();
        SettableFuture<RemoteCalls.PreVoteAck> result = SettableFuture.create();
        client.invokeWithCallback(req, 1000, 1000, payload -> {
            log.info("preVote response={}", payload.toString());
            RemoteCalls.PreVoteAck ack = (RemoteCalls.PreVoteAck) payload;
            result.set(ack);
        });
        return result;
    }

    @Override
    public Future<RemoteCalls.AcquireLeaderResp> acquireLeader(Endpoint endpoint) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.AcquireLeaderReq leaderReq = RemoteCalls.AcquireLeaderReq.newBuilder().setTimestamp(requestId.getCreateTime()).build();
        KvaftMessage<RemoteCalls.AcquireLeaderReq> req = KvaftMessage.<RemoteCalls.AcquireLeaderReq>builder()
                .requestId(requestId.getValue())
                .payload(leaderReq)
                .build();
        SettableFuture<RemoteCalls.AcquireLeaderResp> result = SettableFuture.create();
        client.invokeWithCallback(req, 1000, 1000, payload -> {
            log.info("acquireLeader response={}", payload.toString());
            RemoteCalls.AcquireLeaderResp ack = (RemoteCalls.AcquireLeaderResp) payload;
            result.set(ack);
        });
        return result;
    }

    @Override
    public Future<RemoteCalls.ElectResp> elect(Endpoint endpoint, long term) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        Assert.notNull(client, String.format("could not establish a connection with endpoint=%s", endpoint.toString()));
        RemoteCalls.BindAddress address = RemoteCalls.BindAddress.newBuilder().setHost(endpoint.getIp()).setPort(endpoint.getPort()).build();
        RemoteCalls.ElectReq electReq = RemoteCalls.ElectReq.newBuilder().setTerm(term).setAddress(address).build();
        KvaftMessage<RemoteCalls.ElectReq> req = KvaftMessage.<RemoteCalls.ElectReq>builder()
                .requestId(requestId.getValue())
                .payload(electReq)
                .build();
        SettableFuture<RemoteCalls.ElectResp> result = SettableFuture.create();
        client.invokeWithCallback(req, 1000, 1000, payload -> {
            log.info("acquireLeader response={}", payload.toString());
            RemoteCalls.ElectResp ack = (RemoteCalls.ElectResp) payload;
            result.set(ack);
        });
        return result;
    }
}
