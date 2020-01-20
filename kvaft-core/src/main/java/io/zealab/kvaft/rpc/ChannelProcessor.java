package io.zealab.kvaft.rpc;

import com.google.protobuf.Message;
import io.netty.channel.Channel;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;

/**
 * processor for request received
 *
 * @author LeonWong
 */
public interface ChannelProcessor {

    /**
     * process message
     *
     * @param msg     message entity
     * @param channel connection from endpoint
     */
    void doProcess(KvaftMessage<?> msg, Channel channel);


    /**
     * assert message type is match.
     * <p>
     * if it isn't match yet, it will throw a RuntimeException
     *
     * @param payload payload
     *
     * @throws IllegalArgumentException
     */
    void assertMatch(Message payload) throws IllegalArgumentException;
}
