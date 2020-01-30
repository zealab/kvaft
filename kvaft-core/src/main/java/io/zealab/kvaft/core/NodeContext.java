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

    public void setTermAcked(long termAcked) {
        this.termAcked = termAcked;
    }
}
