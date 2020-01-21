package io.zealab.kvaft.core;

import io.zealab.kvaft.util.Assert;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class Participant {

    private Endpoint endpoint;

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    /**
     * parse config to participant object
     *
     * @param s ip:port string format
     *
     * @return participant
     */
    public static Participant from(String s) {
        String[] data = s.split(":");
        Assert.state(data.length == 2, "you should pass parameter 's' as format like <ip:port>");
        Endpoint endpoint = Endpoint.builder().ip(data[0]).port(Integer.parseInt(data[1])).build();
        return Participant.builder().endpoint(endpoint).build();
    }
}
