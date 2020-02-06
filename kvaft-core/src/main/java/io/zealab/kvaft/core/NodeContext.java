package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public class NodeContext {

    private SignalQueue preVoteConfirmQueue = new SignalQueue();

    private SignalQueue electionConfirmQueue = new SignalQueue();

    private long termAcked;

    private volatile boolean heartbeatOn = false;

    public SignalQueue getPreVoteConfirmQueue() {
        return preVoteConfirmQueue;
    }

    public SignalQueue getElectionConfirmQueue() {
        return electionConfirmQueue;
    }

    public long getTermAcked() {
        return termAcked;
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
