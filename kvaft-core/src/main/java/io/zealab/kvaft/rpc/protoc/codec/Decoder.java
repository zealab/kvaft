package io.zealab.kvaft.rpc.protoc.codec;

import io.zealab.kvaft.rpc.protoc.KvaftMessage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author LeonWong
 */
public interface Decoder {

    /**
     * decoder for KvaftMessage
     *
     * @param data binary data
     *
     * @return obj
     */
    List<KvaftMessage<?>> decode(ByteBuffer data);
}
