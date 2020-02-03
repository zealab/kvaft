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
     * 10000 ms timeout for preVote local spin check
     */
    private int preVoteSpin = 10000;

    /**
     * retry times for pre vote ack
     */
    private int preVoteAckRetry = 10;

    /**
     * 2000 ms timeout for acquireLeader
     */
    private int acquireLeaderTimeout = 2000;

    public boolean isValidParticipant(Participant p) {
        return participants.contains(p);
    }
}
