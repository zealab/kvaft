package io.zealab.kvaft.util;

/**
 * @author LeonWong
 */
public class IpAddressUtil {

    /**
     * convert to ip address from 4bytes
     *
     * @param bytes 4 bytes
     *
     * @return ip address string
     */
    public static String bytesToIpAddress(byte[] bytes) {
        Assert.state(bytes.length == 4, "illegal ip bytes content");
        return String.format("%d.%d.%d.%d", bytes[0] & 0xff, bytes[1] & 0xff, bytes[2] & 0xff, bytes[3] & 0xff);
    }
}
