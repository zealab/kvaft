package io.zealab.kvaft.rpc.protoc.codec;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.ProtocHandleManager;
import io.zealab.kvaft.util.Assert;
import io.zealab.kvaft.util.CrcUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import sun.net.util.IPAddressUtil;

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Kvaft Serializer
 * <p>
 * -----------------------------------------------
 * |  dataSize(32) | node(32) | request id(64)   |
 * -----------------------------------------------
 * |from ip(32)| from port(16)| clazz length(32) |
 * -----------------------------------------------
 * |  clazz meta  | pb  payload | checksum(32)   |
 * -----------------------------------------------
 *
 * @author LeonWong
 */
@Slf4j
public class KvaftProtocolSerializer implements Decoder, Encoder {

    private static class SingletonHolder {
        public static KvaftProtocolSerializer instance = new KvaftProtocolSerializer();
    }

    private KvaftProtocolSerializer() {
        // do nothing
    }

    public static KvaftProtocolSerializer getInstance() {
        return SingletonHolder.instance;
    }

    private static ProtocHandleManager handleManager = new ProtocHandleManager();

    @Override
    public List<KvaftMessage<?>> decode(ByteBuffer data) {
        List<KvaftMessage<?>> result = Lists.newArrayList();
        checkHeaderBound(data);
        while (data.hasRemaining()) {
            data.mark();
            int dataSize = data.getInt();
            if (data.remaining() < dataSize - 4) {
                // not enough data
                data.reset();
                break;
            }
            int node = data.getInt();
            long requestId = data.getLong();
            int fromIp = data.getInt();
            int fromPort = data.getShort();
            int clazzLength = data.getInt();
            byte[] clazzMeta = new byte[clazzLength];
            data.get(clazzMeta);
            int payloadSize = dataSize - clazzLength - getFixHeaderLength();
            Assert.state(payloadSize > 0, "invalid message , cause payload size is illegal");
            byte[] payload = new byte[payloadSize];
            data.get(payload);
            int checksum = data.getInt();
            String clazzName = new String(clazzMeta, UTF_8);
            Message message;
            try {
                MethodHandle handle = handleManager.getHandle(clazzName);
                if (Objects.nonNull(handle)) {
                    message = (Message) handle.invokeExact(payload);
                    result.add(
                            KvaftMessage.builder().checksum(checksum).from(String.format("%s:%d", fromIp, fromPort))
                                    .node(node)
                                    .requestId(requestId)
                                    .payload(message).build()
                    );
                }
            } catch (Throwable e) {
                log.error("KvaftMessage decode reflection error", e);
            }
        }
        return result;
    }

    @Override
    public ByteBuffer encode(@NonNull KvaftMessage<?> kvaftMessage) {
        Message payload = kvaftMessage.payload();
        byte[] clazzMeta = payload.getClass().getName().getBytes(UTF_8);
        int dataSize = payload.toByteArray().length + clazzMeta.length + getFixHeaderLength();
        ByteBuffer encoded = ByteBuffer.allocate(dataSize);
        encoded.putInt(dataSize);
        encoded.putInt(kvaftMessage.node());
        encoded.putLong(kvaftMessage.requestId());
        String[] from = kvaftMessage.from().split(":");
        Assert.state(from.length == 2, "'from' field is illegal");
        encoded.put(IPAddressUtil.textToNumericFormatV4(from[0]));
        encoded.putShort(NumberUtils.toShort(from[1], (short) 0));
        encoded.putInt(clazzMeta.length);
        encoded.put(clazzMeta);
        encoded.put(payload.toByteArray());
        encoded.putInt(CrcUtil.crc32(encoded.array()));
        return encoded;
    }

    private void checkHeaderBound(ByteBuffer data) {
        if (data.capacity() < getFixHeaderLength()) {
            throw new IllegalStateException("checkHeaderBound failed");
        }
    }

    /**
     * get fixed length except payload and clazz meta
     *
     * @return length in bytes
     */
    private int getFixHeaderLength() {
        return 30;
    }


}
