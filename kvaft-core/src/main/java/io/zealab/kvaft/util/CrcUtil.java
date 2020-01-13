package io.zealab.kvaft.util;

/**
 * Cyclic redundancy check
 *
 * @author LeonWong
 */
public class CrcUtil {

    private static final ThreadLocal<Crc32c> CRC_32_THREAD_LOCAL = ThreadLocal.withInitial(Crc32c::new);

    public static int crc32(byte[] data) {
        Crc32c crc32 = CRC_32_THREAD_LOCAL.get();
        crc32.update(data, 0, data.length);
        byte[] crc32Bytes = crc32.getValueAsBytes();
        int value = (crc32Bytes[0] & 0xff) << 24
                | (crc32Bytes[1] & 0xff) << 16
                | (crc32Bytes[2] & 0xff) << 8
                | (crc32Bytes[3] & 0xff);
        crc32.reset();
        return value;
    }
}
