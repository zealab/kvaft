package io.zealab.kvaft.rpc.protoc;

import io.netty.channel.ChannelDuplexHandler;

import java.nio.ByteBuffer;

/**
 * Kvaft Serializer
 * <p>
 * -----------------------------------------------
 * |  dataSize(32) | node(32) | request id(64)   |
 * -----------------------------------------------
 * |  from ip(32) | from port(16) | to ip(32)    |
 * -----------------------------------------------
 * |  to port(16) | checksum(64) | pb encoded    |
 * -----------------------------------------------
 *
 * @author LeonWong
 */
public class KvaftDefaultSerializerHandler extends ChannelDuplexHandler implements Decoder, Encoder {

    @Override
    public KvaftMessage<?> decode(byte[] byteBuffer) {
        return null;
    }

    @Override
    public byte[] encode(KvaftMessage<?> kvaftMessage) {
        ByteBuffer data = ByteBuffer.allocate(kvaftMessage.dataSize());
        data.putInt(kvaftMessage.node());
        data.putInt(kvaftMessage.dataSize());
        return null;
    }
}
