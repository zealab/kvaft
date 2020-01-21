package io.zealab.kvaft.core;

public interface Node extends Initializer {

    /**
     * check is Leader
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
     * starting rpc server
     */
    void start();

    /**
     * shutdown node
     */
    void shutdown();

    /**
     * get leader peer
     *
     * @return peer
     */
    Participant leader();
}
