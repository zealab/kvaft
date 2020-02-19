package io.zealab.kvaft.config;

import com.google.common.collect.Lists;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.Participant;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
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
    private int preVoteConfirmTimeout = 10000;

    /**
     * retry times for pre vote ack
     */
    private int preVoteAckRetry = 10;

    /**
     * 2000 ms timeout for acquireLeader
     */
    private int acquireLeaderTimeout = 2000;

    /**
     * 2000 ms timeout for election
     */
    private int electTimeout = 2000;

    /**
     * 10000 ms timeout for election confirming
     */
    private int electConfirmTimeout = 10000;

    /**
     * 5000 ms interval for heartbeat from leader to follower
     */
    private int heartbeatInterval = 5000;

    public boolean isValidParticipant(Participant p) {
        return participants.contains(p);
    }

    public int getQuorum() {
        return participants.size() / 2 + 1;
    }
}
