package io.zealab.kvaft.config;

import com.google.common.collect.Lists;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Participant;
import lombok.Data;

import java.util.List;

@Data
public class CommonConfig {

    /**
     * those who are participants in this cluster
     */
    private List<Participant> participants = Lists.newArrayList();

    /**
     * TCP server binding endpoint
     */
    private Endpoint bindEndpoint = Endpoint.builder().ip("0.0.0.0").port(2046).build();

    /**
     * 5s timeout for preVote local spin check
     */
    private int preVoteSpin = 5;
}
