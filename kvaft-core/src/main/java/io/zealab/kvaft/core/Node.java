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
     * shutdown node
     */
    void shutdown();

    /**
     * get leader peer
     *
     * @return peer
     */
    Peer leader();
}
