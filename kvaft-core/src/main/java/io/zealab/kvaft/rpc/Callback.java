package io.zealab.kvaft.rpc;

import com.google.protobuf.Message;


/**
 * callback when response
 *
 * @author LeonWong
 */
public interface Callback {

    /**
     * execute response callback function
     */
    void apply(Message resp);
}
