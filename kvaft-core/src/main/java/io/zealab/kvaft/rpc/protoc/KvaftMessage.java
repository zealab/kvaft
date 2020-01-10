package io.zealab.kvaft.rpc.protoc;

import com.google.protobuf.Message;

/**
 * Messages between nodes
 *
 * @author LeonWong
 */
public interface KvaftMessage<T extends Message> {

    /**
     * node id
     *
     * @return
     */
    int node();

    /**
     * request id from client
     *
     * @return request id
     */
    long requestId();

    /**
     * msg from whom
     *
     * @return sender
     */
    String from();

    /**
     * who to send
     *
     * @return
     */
    String to();

    /**
     * generic payload
     *
     * @return payload
     */
    T payload();

    /**
     * checksum
     *
     * @return checksum
     */
    long checksum();

    /**
     * message data size
     *
     * @return
     */
    int dataSize();
}
