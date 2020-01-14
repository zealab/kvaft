package io.zealab.kvaft.rpc.protoc.codec;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.ProtocHandleManager;
import io.zealab.kvaft.util.Assert;
import io.zealab.kvaft.util.CrcUtil;
import io.zealab.kvaft.util.IpAddressUtil;
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

    private static ProtocHandleManager handleManager = ProtocHandleManager.getInstance();

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
            byte[] fromIp = getBytes(data, 4);
            int fromPort = data.getShort();
            int clazzLength = data.getInt();
            byte[] clazzMeta = getBytes(data, clazzLength);

            int payloadSize = dataSize - clazzLength - getFixHeaderLength();
            Assert.state(payloadSize > 0, "invalid message , cause payload size is illegal");
            byte[] payload = getBytes(data, payloadSize);

            // crc32 validation
            checkSum(data, dataSize);
            String clazzName = new String(clazzMeta, UTF_8);
            Message message;
            try {
                MethodHandle handle = handleManager.getHandle(clazzName);
                if (Objects.nonNull(handle)) {
                    message = (Message) handle.invokeWithArguments(payload);
                    KvaftMessage<?> kvaftMessage = KvaftMessage.builder().from(String.format("%s:%d", IpAddressUtil.bytesToIpAddress(fromIp), fromPort))
                            .node(node)
                            .requestId(requestId)
                            .payload(message).build();
                    result.add(kvaftMessage);
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
            throw new IllegalStateException("checking protocol head is bounded, but failed");
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


    private boolean checkSum(ByteBuffer data, int dataSize) {
        // crc32 validation
        int pos = data.position();
        data.reset();
        byte[] srcData = new byte[dataSize - 4];
        data.get(srcData);
        data.position(pos);
        int checksum = data.getInt();
        return checkSum(srcData, checksum);
    }

    /**
     * crc32 check
     *
     * @param data  src data
     * @param crc32 remainder
     *
     * @return check result
     */
    private boolean checkSum(byte[] data, int crc32) {
        int value = CrcUtil.crc32(data);
        return crc32 == value;
    }

    /**
     * byte buffer get bytes
     *
     * @param data
     * @param size
     *
     * @return
     */
    private byte[] getBytes(ByteBuffer data, int size) {
        byte[] result = new byte[size];
        data.get(result);
        return result;
    }
}
