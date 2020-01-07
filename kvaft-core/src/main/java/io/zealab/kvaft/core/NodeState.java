package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public enum NodeState {
    /**
     * as leader
     */
    LEADER,
    /**
     * as follower
     */
    FOLLOWER,

    /**
     * as candidate
     */
    CANDIDATE;
}
