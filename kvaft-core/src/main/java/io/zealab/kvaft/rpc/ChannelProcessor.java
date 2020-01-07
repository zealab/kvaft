package io.zealab.kvaft.rpc;

/**
 * processor for request received
 *
 * @author LeonWong
 */
public interface ChannelProcessor<T> {

    /**
     * process message
     */
    void doProcess(Message<T> msg);
}
