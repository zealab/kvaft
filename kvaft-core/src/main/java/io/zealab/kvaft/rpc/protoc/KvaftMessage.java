package io.zealab.kvaft.rpc.protoc;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.ToString;

import java.io.Serializable;

/**
 * communication message entity
 * <p>
 * -----------------------------------------------
 * |  magic code(32) |      dataSize(32)         |
 * -----------------------------------------------
 * |    request id(64)   | clazz length(32)      |
 * -----------------------------------------------
 * |  clazz meta  | pb  payload | checksum(32)   |
 * -----------------------------------------------
 *
 * @author LeonWong
 */
@Builder
@ToString
public class KvaftMessage<T extends Message> implements Serializable {

    private long requestId;

    private T payload;

    /**
     * request id from client
     *
     * @return request id
     */
    public long requestId() {
        return requestId;
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
