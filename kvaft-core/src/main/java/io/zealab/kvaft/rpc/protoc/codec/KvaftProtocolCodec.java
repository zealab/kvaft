package io.zealab.kvaft.rpc.protoc.codec;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.ProtocHandleManager;
import io.zealab.kvaft.util.Assert;
import io.zealab.kvaft.util.Crc32c;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Kvaft Serializer
 * <p>
 * -----------------------------------------------
 * |  dataSize(32) |       request id(64)        |
 * -----------------------------------------------
 * |  clazz length(32) |    clazz meta           |
 * -----------------------------------------------
 * |   pb  payload              | checksum(32)   |
 * -----------------------------------------------
 *
 * @author LeonWong
 */
@Slf4j
public class KvaftProtocolCodec implements Decoder, Encoder {

    private ProtocHandleManager handleManager = ProtocHandleManager.getInstance();

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
            long requestId = data.getLong();
            int clazzLength = data.getInt();
            byte[] clazzMeta = getBytes(data, clazzLength);

            int payloadSize = dataSize - clazzLength - getFixHeaderLength();
            Assert.state(payloadSize > 0, "invalid message , cause payload size is illegal");
            byte[] payload = getBytes(data, payloadSize);

            // crc32 validation
            boolean isValid = checkSum(data, dataSize);
            if (isValid) {
                try {
                    String clazzName = new String(clazzMeta, UTF_8);
                    MethodHandle handle = handleManager.getHandle(clazzName);
                    if (Objects.nonNull(handle)) {
                        Message message = (Message) handle.invokeWithArguments(payload);
                        KvaftMessage<?> kvaftMessage = KvaftMessage.builder()
                                .requestId(requestId)
                                .payload(message).build();
                        result.add(kvaftMessage);
                    }
                } catch (Throwable e) {
                    log.error("KvaftMessage decode reflection error", e);
                }
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
        encoded.putLong(kvaftMessage.requestId());
        encoded.putInt(clazzMeta.length);
        encoded.put(clazzMeta);
        encoded.put(payload.toByteArray());
        encoded.flip();
        int crc = Crc32c.crc32(getBytes(encoded, dataSize - 4));
        encoded.limit(dataSize);
        encoded.position(dataSize - 4);
        encoded.putInt(crc);
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
        return 20;
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
        int value = Crc32c.crc32(data);
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
        data.get(result, 0, size);
        return result;
    }
}
