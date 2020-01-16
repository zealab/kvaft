package io.zealab.kvaft.util;

import io.netty.channel.Channel;

import java.net.SocketAddress;

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

    /**
     * convert channel to ip address
     *
     * @param channel netty channel
     *
     * @return
     */
    public static String convertChannelRemoteAddress(Channel channel) {
        if (null == channel) {
            return "";
        }
        final SocketAddress remote = channel.remoteAddress();
        final String addr = remote.toString();
        if (addr.charAt(0) == '/') {
            return addr.substring(1);
        }
        return addr;
    }
}
