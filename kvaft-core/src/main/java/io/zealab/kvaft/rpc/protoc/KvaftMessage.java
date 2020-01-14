package io.zealab.kvaft.rpc.protoc;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.ToString;

/**
 * communication message entity
 * <p>
 * -----------------------------------------------
 * |  dataSize(32) | node(32) | request id(64)   |
 * -----------------------------------------------
 * |from ip(32)| from port(16)| clazz length(32) |
 * -----------------------------------------------
 * |  clazz meta  | pb  payload | checksum(32)   |
 * -----------------------------------------------
 *
 * @author LeonWong
 */
@Builder
@ToString
public class KvaftMessage<T extends Message> {

    private int node;

    private long requestId;

    private String from;

    private T payload;

    /**
     * node id
     *
     * @return node
     */
    public int node() {
        return node;
    }

    /**
     * request id from client
     *
     * @return request id
     */
    public long requestId() {
        return requestId;
    }

    /**
     * msg from whom
     *
     * @return sender
     */
    public String from() {
        return from;
    }

    /**
     * generic payload
     *
     * @return payload
     */
    public T payload() {
        return payload;
    }
}
