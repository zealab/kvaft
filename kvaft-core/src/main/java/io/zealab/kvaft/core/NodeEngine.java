package io.zealab.kvaft.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.zealab.kvaft.config.CommonConfig;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.NioServer;
import io.zealab.kvaft.rpc.client.Stub;
import io.zealab.kvaft.rpc.client.StubImpl;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
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
import java.util.concurrent.*;
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

    private final AtomicLong term = new AtomicLong(0L);

    private volatile Participant leader;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final NodeContext context = new NodeContext();

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    private final static ExecutorService asyncExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2,
            Runtime.getRuntime().availableProcessors() * 2,
            30 * 10, TimeUnit.SECONDS,
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

    /**
     * It will authorize when the follow two conditions satisfied:
     *
     * 1. Term which the peer offers must greater than or equal current term;
     * 2. Only one of all requests in the same term will be authorized.
     *
     * @param peer client
     * @param offerTerm offer term
     */
    @Override
    public void handlePreVoteRequest(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        boolean authorized = false;
        synchronized (term) {
            long currTerm = currTerm();
            if (currTerm <= offerTerm && context.getTermAcked() < offerTerm) {
                term.set(offerTerm);
                context.setTermAcked(offerTerm);
                authorized = true;
            }
        }
        RemoteCalls.PreVoteAck preVoteAck = RemoteCalls.PreVoteAck.newBuilder().setAuthorized(authorized).build();
        KvaftMessage<RemoteCalls.PreVoteAck> kvaftMessage = KvaftMessage.<RemoteCalls.PreVoteAck>builder().payload(preVoteAck).requestId(requestId).build();
        channel.writeAndFlush(kvaftMessage);
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
        // starting rpc server
        Endpoint bindAddress = commonConfig.getBindEndpoint();
        server = new NioServer(bindAddress.getIp(), bindAddress.getPort());
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
        asyncExecutor.execute(new PreVoteConfirmingTask(term));
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
                long termVal;
                synchronized (term) {
                    termVal = term.incrementAndGet();
                }
                log.info("start to broadcast pre voting msg to the other participants");
                // not decide which node is leader
                context.getPreVoteConfirmQueue().updateTerm(termVal);
                List<Participant> participants = commonConfig.getParticipants();
                // starting to check pre vote acknowledges
                startPreVoteSpinTask(termVal);
                participants.parallelStream().filter(e -> !e.isOntology()).forEach(
                        p -> {
                            Endpoint endpoint = p.getEndpoint();
                            Future<RemoteCalls.PreVoteAck> future = stub.preVoteReq(endpoint, termVal);
                            // Waiting for pre vote acknowledges from other participants
                            for (int i = 0; i < commonConfig.getPreVoteAckRetry(); i++) {
                                if (future.isDone()) {
                                    break;
                                }
                                // sleep for a second
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    log.error("Checking future was interrupted");
                                }
                            }
                            try {
                                if (future.isDone()) {
                                    RemoteCalls.PreVoteAck ack = future.get();
                                    log.info("PreVote acknowledge endpoint={},content={}", endpoint.toString(), ack.toString());
                                    if (ack.getAuthorized()) {
                                        context.getPreVoteConfirmQueue().addSignalIfNx(endpoint, termVal);
                                    }
                                } else {
                                    log.info("it's timeout for waiting response");
                                }
                            } catch (InterruptedException | ExecutionException e) {
                                log.error("There're some problems when retrieving result from the preVoteAck", e.getCause());
                            }
                        }
                );
            }

        }
    }

    /**
     * spin check for preVote ack info
     */
    public class PreVoteConfirmingTask implements Runnable {

        private final long term;

        public PreVoteConfirmingTask(long term) {
            this.term = term;
        }

        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            while (System.currentTimeMillis() - begin < commonConfig.getPreVoteSpin()) {
                int quorum = (commonConfig.getParticipants().size() + 1) / 2 + 1;
                long currTerm = currTerm();
                if (currTerm == term && context.getPreVoteConfirmQueue().size() >= quorum) {
                    electSelfNode();
                    return;
                }
                // sleep for a second
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("PreVoteConfirmingTask was interrupted");
                }
            }
            log.warn("PreVoteConfirmingTask timeout in {} ms", commonConfig.getPreVoteSpin());
            startSleepTimeoutTask();
        }
    }
}
