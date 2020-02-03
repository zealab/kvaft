package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public enum NodeState {

    /**
     * as a follower
     */
    FOLLOWING,

    /**
     * as a candidate who may become leader
     */
    ELECTING,

    /**
     * as a leader
     */
    ELECTED,
}
