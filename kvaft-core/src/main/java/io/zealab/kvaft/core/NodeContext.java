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
    private long lastTerm;

    /**
     * is heartbeat task on?
     */
    private volatile boolean heartbeatOn = false;

    /**
     * is sleep timeout task on?
     */
    private volatile boolean sleepTimeoutTaskOn = false;

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

    public long getLastTerm() {
        return lastTerm;
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
     *
     * @return
     */
    public boolean isAuthorizable(long offerTerm) {
        return this.lastTerm < offerTerm;
    }

    public void setLastTerm(long lastTerm) {
        this.lastTerm = lastTerm;
    }

    public boolean isHeartbeatOn() {
        return heartbeatOn;
    }

    public void turnOffHeartbeat() {
        this.heartbeatOn = false;
    }

    public void turnOnHeartbeat() {
        this.heartbeatOn = true;
    }

    public void turnOnSleepTimeout() {
        this.sleepTimeoutTaskOn = true;
    }

    public void turnOffSleepTimeout() {
        this.sleepTimeoutTaskOn = false;
    }

    public boolean isSleepTimeoutTaskOn(){
        return sleepTimeoutTaskOn;
    }
}
