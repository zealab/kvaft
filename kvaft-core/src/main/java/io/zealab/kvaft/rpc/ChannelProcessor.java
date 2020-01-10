package io.zealab.kvaft.rpc;

import com.google.protobuf.Message;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;

/**
 * processor for request received
 *
 * @author LeonWong
 */
public interface ChannelProcessor<T extends Message> {

    /**
     * process message
     *
     * @param msg message entity
     */
    void doProcess(KvaftMessage<T> msg);
}
