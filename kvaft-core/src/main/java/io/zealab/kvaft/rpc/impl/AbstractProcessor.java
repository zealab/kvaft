package io.zealab.kvaft.rpc.impl;

import com.google.protobuf.Message;
import io.zealab.kvaft.core.Peer;
import io.zealab.kvaft.rpc.ChannelProcessor;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * server side's abstract processor
 *
 * @author LeonWong
 */
@Slf4j
public abstract class AbstractProcessor<T extends Message> implements ChannelProcessor<T> {

    /**
     * handle payload
     *
     * @param payload
     */
    protected abstract void doProcess0(Peer peer, T payload);

    /**
     * pre process
     *
     * @param msg message entity
     */
    public void doProcess(KvaftMessage<T> msg) {
        checkMsg(msg);
        doProcess0(Peer.builder().nodeId(msg.node()).endpoint(msg.from()).build(), msg.payload());
    }

    private void checkMsg(KvaftMessage<T> msg) {
        // TODO verify request id & checksum & nodeid
    }
}
