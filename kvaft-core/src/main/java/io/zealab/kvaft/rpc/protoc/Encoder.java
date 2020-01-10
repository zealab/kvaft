package io.zealab.kvaft.rpc.protoc;

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
    byte[] encode(KvaftMessage<?> kvaftMessage);
}
