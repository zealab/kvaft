package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public class NodeContext {

    private SignalQueue preVoteConfirmQueue = new SignalQueue();

    private long termAcked;

    public SignalQueue getPreVoteConfirmQueue() {
        return preVoteConfirmQueue;
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
