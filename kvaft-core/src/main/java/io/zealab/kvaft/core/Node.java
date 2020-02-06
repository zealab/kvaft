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
     * It will authorize when the follow three conditions satisfied:
     *
     * 1. Term which the peer offers must greater than or equal current term;
     * 2. Only one of all requests in the same term will be authorized;
     * 3. Ensure that current node is NOT in the ELECTING and ELECTED state.
     *
     * When A has authorized B (B may be A itself) in the term X,
     *
     * @param peer       client
     * @param requestId  requestId
     * @param offerTerm  offer term
     */
    void handlePreVoteRequest(Peer peer, long requestId, long offerTerm);

    /**
     * The majority agreed node will broadcast the election request which this method could handle
     *
     * @param peer       client
     * @param requestId  requestId
     * @param offerTerm  offer term
     */
    void handleElectRequest(Peer peer, long requestId, long offerTerm);

    /**
     * This method handles heartbeat from leader which uses it for maintaining replicators relationships
     *
     * @param peer       client
     * @param requestId  requestId
     * @param offerTerm  offerTerm
     */
    void handleHeartbeat(Peer peer, long requestId, long offerTerm);
}
