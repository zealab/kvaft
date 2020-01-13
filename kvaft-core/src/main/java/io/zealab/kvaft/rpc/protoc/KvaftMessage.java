package io.zealab.kvaft.rpc.protoc;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.ToString;

/**
 * Messages between nodes
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

    private int checksum;

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

    /**
     * checksum crc32
     *
     * @return crc32 value
     */
    public int checksum() {
        return checksum;
    }
}
