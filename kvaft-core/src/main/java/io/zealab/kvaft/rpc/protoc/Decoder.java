package io.zealab.kvaft.rpc.protoc;

/**
 * @author LeonWong
 */
public interface Decoder {

    /**
     * decoder for KvaftMessage
     *
     * @param byteBuf binary data
     *
     * @return obj
     */
    KvaftMessage<?> decode(byte[] byteBuf);
}
