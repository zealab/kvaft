package io.zealab.kvaft.rpc.protoc.codec;

import io.zealab.kvaft.rpc.protoc.KvaftMessage;

import java.nio.ByteBuffer;

/**
 * @author LeonWong
 */
public interface Encoder {

    /**
     * encoder for KvaftMessage
     *
     * @param kvaftMessage msg
     *
     * @return binary data
     */
    ByteBuffer encode(KvaftMessage<?> kvaftMessage);
}
