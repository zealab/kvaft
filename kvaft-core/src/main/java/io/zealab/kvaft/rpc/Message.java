package io.zealab.kvaft.rpc;

import io.zealab.kvaft.util.Endpoint;

/**
 * Messages between nodes
 *
 * @author LeonWong
 */
public interface Message<T> {

    /**
     * retrieve msg type
     *
     * @return msg type
     */
    MsgType msgType();

    /**
     * msg from whom
     *
     * @return sender
     */
    Endpoint from();

    /**
     * generic payload
     *
     * @return payload
     */
    T payload();

    /**
     * current term value from the sender
     *
     * @return term value
     */
    Long term();
}
