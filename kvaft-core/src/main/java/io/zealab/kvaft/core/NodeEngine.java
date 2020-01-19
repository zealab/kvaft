package io.zealab.kvaft.core;

import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.client.Stub;
import io.zealab.kvaft.rpc.client.StubImpl;

/**
 * @author LeonWong
 */
public class NodeEngine implements Node {

    private final static Stub stub = new StubImpl();

    private volatile NodeState state;

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    @Override
    public boolean isLeader() {
        return false;
    }

    @Override
    public Long currTerm() {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Peer leader() {
        return null;
    }

    @Override
    public void init() {
        GlobalScanner scanner = new GlobalScanner();
        scanner.init();
        state = NodeState.FOLLOWER;
    }
}
