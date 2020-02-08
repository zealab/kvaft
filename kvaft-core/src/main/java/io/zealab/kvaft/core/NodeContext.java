package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public class NodeContext {

    /**
     * Pre-Voting stage response confirm queue
     */
    private SignalQueue preVoteConfirmQueue = new SignalQueue();

    /**
     * Electing stage response confirm queue
     */
    private SignalQueue electionConfirmQueue = new SignalQueue();

    /**
     * Avoiding authorized pre-vote request in the same term and same node
     */
    private long termAcked;

    /**
     * is heartbeat task on?
     */
    private volatile boolean heartbeatOn = false;

    public SignalQueue getPreVoteConfirmQueue() {
        return preVoteConfirmQueue;
    }

    public SignalQueue getElectionConfirmQueue() {
        return electionConfirmQueue;
    }

    public int electionConfirmQueueSize() {
        return electionConfirmQueue.size();
    }

    public void resetElectionConfirmQueue(long term) {
        electionConfirmQueue.updateTerm(term);
    }

    public void addElectionConfirmNx(Endpoint endpoint, long term) {
        electionConfirmQueue.addSignalIfNx(endpoint, term);
    }

    public long getTermAcked() {
        return termAcked;
    }

    public int preVoteConfirmQueueSize() {
        return preVoteConfirmQueue.size();
    }

    public void resetPreVoteConfirmQueue(long term) {
        preVoteConfirmQueue.updateTerm(term);
    }

    public void addPreVoteConfirmNx(Endpoint endpoint, long term) {
        preVoteConfirmQueue.addSignalIfNx(endpoint, term);
    }

    /**
     * is authorizable for this offer term
     *
     * @param offerTerm
     * @return
     */
    public boolean isAuthorizable(long offerTerm) {
        return termAcked < offerTerm;
    }

    public void setTermAcked(long termAcked) {
        this.termAcked = termAcked;
    }

    public boolean isHeartbeatOn() {
        return heartbeatOn;
    }

    public NodeContext setHeartbeatOn(boolean heartbeatOn) {
        this.heartbeatOn = heartbeatOn;
        return this;
    }

    /**
     * get quorum qty
     *
     * @param totalQty node qty
     * @return
     */
    public int getQuorum(int totalQty) {
        return totalQty / 2 + 1;
    }
}
