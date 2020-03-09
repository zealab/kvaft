package io.zealab.kvaft.rpc.client;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;


@Slf4j
public class StubImpl extends AbstractStub {

    @Override
    public Future<RemoteCalls.HeartbeatAck> heartbeat(Endpoint endpoint, long term) {
        RemoteCalls.Heartbeat heartbeat = RemoteCalls.Heartbeat.newBuilder().setTerm(term).setTimestamp(System.currentTimeMillis()).build();
        return doInvoke(endpoint, heartbeat);
    }

    @Override
    public Future<RemoteCalls.PreVoteAck> preVote(Endpoint endpoint, long term) {
        RemoteCalls.PreVoteReq preVoteReq = RemoteCalls.PreVoteReq.newBuilder().setTerm(term).build();
        return doInvoke(endpoint, preVoteReq);
    }

    @Override
    public Future<RemoteCalls.AcquireLeaderResp> acquireLeader(Endpoint endpoint) {
        RemoteCalls.AcquireLeaderReq leaderReq = RemoteCalls.AcquireLeaderReq.newBuilder().setTimestamp(System.currentTimeMillis()).build();
        return doInvoke(endpoint, leaderReq);
    }

    @Override
    public Future<RemoteCalls.ElectResp> elect(Endpoint endpoint, long term) {
        RemoteCalls.BindAddress address = RemoteCalls.BindAddress.newBuilder().setHost(endpoint.getIp()).setPort(endpoint.getPort()).build();
        RemoteCalls.ElectReq electReq = RemoteCalls.ElectReq.newBuilder().setTerm(term).setAddress(address).build();
        return doInvoke(endpoint, electReq);
    }

    @Override
    public void stepDown(Endpoint endpoint, long term) {
        RemoteCalls.StepDownMsg stepDownMsg = RemoteCalls.StepDownMsg.newBuilder().setTerm(term).build();
        doInvokeOneWay(endpoint, stepDownMsg, 5000, 1000);
    }
}
