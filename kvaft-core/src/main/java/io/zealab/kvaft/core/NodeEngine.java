package io.zealab.kvaft.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.client.Stub;
import io.zealab.kvaft.rpc.client.StubImpl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author LeonWong
 */
public class NodeEngine implements Node {

    private final static Stub stub = new StubImpl();

    private volatile NodeState state;

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    private final static ExecutorService asyncExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2, Runtime.getRuntime().availableProcessors() * 2, 30 * 10, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2 << 16),
            new ThreadFactoryBuilder().setNameFormat("node-engine-async-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

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
        startSleepTimeoutTask();
    }

    public void startSleepTimeoutTask() {
        asyncExecutor.execute(new SleepTimeoutTask());
    }

    /**
     * When sleep thread task awake, it will broadcast preVote message to every node.
     */
    public class SleepTimeoutTask implements Runnable {

        @Override
        public void run() {
            //TODO sleep in random seconds
        }
    }
}
