package io.zealab.kvaft.rpc.protoc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Kvaft Codec Handler
 * <p>
 * based on KvaftDefaultCodec
 *
 * @author LeonWong
 */
@Slf4j
public class KvaftDefaultCodecHandler extends ByteToMessageCodec<KvaftMessage<?>> {

    private KvaftProtocolCodec codec = CodecFactory.getInstance();

    @Override
    protected void encode(ChannelHandlerContext ctx, KvaftMessage<?> msg, ByteBuf out) throws Exception {
        ByteBuffer encoded = codec.encode(msg);
        out.writeBytes(encoded);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ByteBuffer nioBuffer = in.nioBuffer();
        try {
            List<KvaftMessage<?>> messages = codec.decode(nioBuffer);
            in.readBytes(nioBuffer.position());
            out.addAll(messages);
        } catch (Exception e) {
            log.error("decode failed when handle message");
            // read remain message
            in.readBytes(in.readableBytes());
        }
    }
}
