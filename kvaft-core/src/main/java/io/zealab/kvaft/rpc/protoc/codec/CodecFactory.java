package io.zealab.kvaft.rpc.protoc.codec;

/**
 * @author LeonWong
 */
public class CodecFactory {

    public static KvaftProtocolCodec getInstance() {
        return new KvaftProtocolCodec();
    }
}
