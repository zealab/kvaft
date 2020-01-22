package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.core.ProcessorType;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;

/**
 * process heartbeats from client
 *
 * @author LeonWong
 */
@Processor(handleType = ProcessorType.REQ, messageClazz = RemoteCalls.Heartbeat.class)
public class HeartbeatProcessor extends RequestProcessor<RemoteCalls.Heartbeat> {

    @Override
    protected void doProcess0(Peer peer, RemoteCalls.Heartbeat payload) {
        long timestamp = System.currentTimeMillis();
        peer.setLastHbTime(timestamp);
    }
}
