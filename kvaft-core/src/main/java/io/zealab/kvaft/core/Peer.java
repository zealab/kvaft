package io.zealab.kvaft.core;

import io.netty.channel.Channel;
import io.zealab.kvaft.util.IpAddressUtil;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author LeonWong
 */
@Data
@Builder
@Slf4j
public class Peer {

    /**
     * ip + port
     */
    private Endpoint endpoint;


    /**
     * last heartbeat time
     */
    private volatile long lastHbTime;

    /**
     * connection
     */
    private Channel channel;

    /**
     * get node id
     *
     * @return
     */
    public String nodeId() {
        return endpoint.toString();
    }

    public static Peer from(Channel channel) {
        String address = IpAddressUtil.convertChannelRemoteAddress(channel);
        if (address.contains(":")) {
            String[] data = address.split(":");
            Endpoint endpoint = Endpoint.builder().ip(data[0]).port(Integer.parseInt(data[1])).build();
            return Peer.builder().channel(channel).lastHbTime(System.currentTimeMillis()).endpoint(endpoint).build();
        } else {
            log.error("detected a bug here !");
            return null;
        }
    }
}
