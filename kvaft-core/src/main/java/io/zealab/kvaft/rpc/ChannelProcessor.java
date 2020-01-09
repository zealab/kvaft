package io.zealab.kvaft.rpc;

/**
 * processor for request received
 *
 * @author LeonWong
 */
public interface ChannelProcessor<T> {

    /**
     * process message
     *
     * @param msg message entity
     */
    void doProcess(Message<T> msg);
}
