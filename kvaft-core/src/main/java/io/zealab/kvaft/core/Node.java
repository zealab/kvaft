package io.zealab.kvaft.core;

public interface Node extends Initializer {

    /**
     * check if it's a Leader
     *
     * @return
     */
    boolean isLeader();

    /**
     * current term
     *
     * @return
     */
    Long currTerm();

    /**
     * starting this node engine
     */
    void start();

    /**
     * shutdown node engine
     */
    void shutdown();

    /**
     * get leader peer
     *
     * @return peer
     */
    Participant leader();

    /**
     * preVote request handle method
     *
     * @param peer       client
     * @param requestId  requestId
     * @param term       offer term
     */
    void handlePreVoteRequest(Peer peer, long requestId, long term);
}
