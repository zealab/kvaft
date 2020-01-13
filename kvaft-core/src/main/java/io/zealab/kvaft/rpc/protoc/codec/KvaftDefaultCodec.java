package io.zealab.kvaft.rpc.protoc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Kvaft Codec Handler
 * <p>
 * based on KvaftDefaultCodec
 *
 * @author LeonWong
 */
public class KvaftDefaultCodec extends ByteToMessageCodec<KvaftMessage<?>> {

    private KvaftProtocolSerializer serializer = KvaftProtocolSerializer.getInstance();


    @Override
    protected void encode(ChannelHandlerContext ctx, KvaftMessage<?> msg, ByteBuf out) throws Exception {
        ByteBuffer encoded = serializer.encode(msg);
        out.writeBytes(encoded);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ByteBuffer nioBuffer = in.nioBuffer();
        List<KvaftMessage<?>> messages = serializer.decode(nioBuffer);
        in.readBytes(nioBuffer.position());
        out.addAll(messages);
    }
}
