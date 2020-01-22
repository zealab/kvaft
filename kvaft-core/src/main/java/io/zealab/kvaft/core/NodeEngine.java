package io.zealab.kvaft.core;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.zealab.kvaft.config.CommonConfig;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.NioServer;
import io.zealab.kvaft.rpc.client.ReplicatorManager;
import io.zealab.kvaft.rpc.client.Stub;
import io.zealab.kvaft.rpc.client.StubImpl;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * node engine core
 *
 * @author LeonWong
 */
@Slf4j
public class NodeEngine implements Node {

    private final CommonConfig commonConfig = new CommonConfig();

    private String configFileLocation = "kvaft.yml";

    private final static Stub stub = new StubImpl();

    private NioServer server;

    private AtomicLong term = new AtomicLong(0L);

    private volatile Participant leader;

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private Set<String> ackQueue = Sets.newConcurrentHashSet();

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    private final static ReplicatorManager replicatorManager = ReplicatorManager.getInstance();

    private final static ExecutorService asyncExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2, Runtime.getRuntime().availableProcessors() * 2, 30 * 10, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2 << 16),
            new ThreadFactoryBuilder().setNameFormat("node-engine-async-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

    @Override
    public boolean isLeader() {
        Participant leader = leader();
        return leader != null && leader.isOntology();
    }

    @Override
    public Long currTerm() {
        return term.get();
    }

    @Override
    public void start() {
        server.start();
        startSleepTimeoutTask();
    }

    @Override
    public void shutdown() {
        // TODO
    }

    @Override
    public Participant leader() {
        return leader;
    }

    @Override
    public void handlePreVoteRequest(Peer peer, long term) {
        //TODO
    }

    @Override
    public void init() {
        // read the config file
        parseConfigFile();
        // scanning all package classes for configuration
        GlobalScanner scanner = new GlobalScanner();
        scanner.init();
        // binding node
        processManager.bindNode(this);
        replicatorManager.bindNode(this);
        // starting rpc server
        server = new NioServer(commonConfig.getBindEndpoint().getIp(), commonConfig.getBindEndpoint().getPort());
        server.init();
    }

    /**
     * begin to elect itself
     */
    public void electSelfNode() {
        // TODO
    }

    /**
     * starting a sleep timeout task
     */
    public void startSleepTimeoutTask() {
        asyncExecutor.execute(new SleepTimeoutTask());
    }

    /**
     * starting a pre vote timeout task
     */
    public void startPreVoteSpinTask(long term) {
        asyncExecutor.execute(new PreVoteSpinTask(term));
    }

    public void setConfigFileLocation(String location) {
        this.configFileLocation = location;
    }

    /**
     * reset leader when heartbeat timeout
     */
    private void resetLeader() {
        Lock wl = rwLock.writeLock();
        try {
            wl.lock();
            this.leader = null;
            startSleepTimeoutTask();
        } finally {
            wl.unlock();
        }
    }

    private void parseConfigFile() {
        Yaml yaml = new Yaml();
        InputStream configStream;
        try {
            configStream = Files.newInputStream(Paths.get(configFileLocation));
        } catch (IOException e) {
            log.warn("could not find any file from Files.newInputStream ");
            configStream = getClass().getClassLoader().getResourceAsStream(configFileLocation);
        }
        Assert.notNull(configStream, "The config file could be not existed");
        Map<String, Object> configs = yaml.load(configStream);

        Endpoint endpoint = this.commonConfig.getBindEndpoint();
        try {
            InetAddress local = InetAddress.getLocalHost();
            endpoint.setIp(local.getHostAddress());
        } catch (UnknownHostException e) {
            log.error("unknown host", e);
        }
        endpoint.setPort((int) configs.getOrDefault("port", 2046));
        endpoint.setIp((String) configs.getOrDefault("host", endpoint.getIp()));

        String patcpsConfig = (String) configs.get("participants");
        Assert.notNull(patcpsConfig, "participants field cannot be null");
        Arrays.asList(patcpsConfig.split(",")).forEach(
                e -> this.commonConfig.getParticipants().add(Participant.from(e, false))
        );
        this.commonConfig.getParticipants().add(Participant.from(endpoint.toString(), true));
    }

    /**
     * When the sleep thread task awake, it will broadcast preVote message to every node.
     */
    public class SleepTimeoutTask implements Runnable {

        @Override
        public void run() {
            Random random = new Random();
            int r = random.nextInt(5000);
            try {
                Thread.sleep(r);
            } catch (InterruptedException e) {
                log.error("SleepTimeoutTask was interrupted", e);
            }
            if (leader == null) {
                long termVal = term.incrementAndGet();
                log.info("start to broadcast preVote msg to the other participants");
                // not decide which node is leader
                List<Participant> participants = commonConfig.getParticipants();
                participants.parallelStream().filter(e -> !e.isOntology()).forEach(
                        p -> {
                            Endpoint endpoint = p.getEndpoint();
                            stub.preVoteReq(endpoint, termVal);
                        }
                );
                startPreVoteSpinTask(termVal);
            }
        }
    }

    /**
     * spin check for preVote ack info
     */
    public class PreVoteSpinTask implements Runnable {

        private final long term;

        public PreVoteSpinTask(long term) {
            this.term = term;
        }

        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            while (System.currentTimeMillis() - begin < commonConfig.getPreVoteSpin()) {
                int quorum = (commonConfig.getParticipants().size() + 1) / 2 + 1;
                long currTerm = currTerm();
                if (currTerm == term && ackQueue.size() >= quorum) {
                    electSelfNode();
                    return;
                }
            }
            log.warn("PreVoteSpinTask timeout in {} seconds", commonConfig.getPreVoteSpin());
            startSleepTimeoutTask();
        }
    }
}
