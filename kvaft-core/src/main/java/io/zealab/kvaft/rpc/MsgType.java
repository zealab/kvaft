package io.zealab.kvaft.rpc;

/**
 * message command type
 *
 * @author LeonWong
 */
public enum MsgType {
    PRE_VOTE_REQ,
    PRE_VOTE_RESP,
    VOTE_REQ,
    VOTE_RESP,
    HEARTBEAT;
}
