package io.zealab.kvaft.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.zealab.kvaft.config.CommonConfig;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.NioServer;
import io.zealab.kvaft.rpc.client.ReplicatorManager;
import io.zealab.kvaft.rpc.client.Stub;
import io.zealab.kvaft.rpc.client.StubImpl;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import io.zealab.kvaft.util.Assert;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;
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
import java.util.stream.Collectors;

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

    private volatile NodeState state = NodeState.FOLLOWING;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final NodeContext context = new NodeContext();

    private final static ChannelProcessorManager processManager = ChannelProcessorManager.getInstance();

    private final static ReplicatorManager replicatorManager = ReplicatorManager.getInstance();

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
        Participant leader = acquireLeader();
        if (leader == null) {
            startSleepTimeoutTask();
        }
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
    public void handlePreVoteRequest(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        boolean authorized = false;
        synchronized (term) {
            long currTerm = currTerm();
            boolean doAuthorize = currTerm <= offerTerm
                    && context.isAuthorizable(offerTerm)
                    && state.compareTo(NodeState.ELECTING) < 0;
            if (doAuthorize) {
                term.set(offerTerm);
                context.setTermAcked(offerTerm);
                state = NodeState.ELECTING;
                authorized = true;
            }
        }
        RemoteCalls.PreVoteAck preVoteAck = RemoteCalls.PreVoteAck.newBuilder().setAuthorized(authorized).build();
        KvaftMessage<RemoteCalls.PreVoteAck> kvaftMessage = KvaftMessage.<RemoteCalls.PreVoteAck>builder().payload(preVoteAck).requestId(requestId).build();
        channel.writeAndFlush(kvaftMessage);
    }

    @Override
    public void handleElectRequest(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        boolean authorized = false;
        synchronized (term) {
            long currTerm = currTerm();
            boolean doAuthorize = currTerm <= offerTerm
                    && state.compareTo(NodeState.ELECTING) == 0;
            if (doAuthorize) {
                term.set(offerTerm);
                state = NodeState.ELECTED;
                authorized = true;
            }
        }
        RemoteCalls.ElectResp electResp = RemoteCalls.ElectResp.newBuilder().setTerm(offerTerm).setAuthorized(authorized).build();
        KvaftMessage<RemoteCalls.ElectResp> kvaftMessage = KvaftMessage.<RemoteCalls.ElectResp>builder().payload(electResp).requestId(requestId).build();
        channel.writeAndFlush(kvaftMessage);
    }

    @Override
    public void handleHeartbeat(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        if (offerTerm == term.get() && state == NodeState.ELECTED) {
            RemoteCalls.HeartbeatAck heartbeatAck = RemoteCalls.HeartbeatAck.newBuilder().setTimestamp(System.currentTimeMillis()).build();
            KvaftMessage<RemoteCalls.HeartbeatAck> kvaftMessage = KvaftMessage.<RemoteCalls.HeartbeatAck>builder().payload(heartbeatAck).requestId(requestId).build();
            channel.writeAndFlush(kvaftMessage);
        }
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
    public void electItselfNode(final long term) {
        log.info("Leader election starting...");
        state = NodeState.ELECTING;
        startElectionConfirmingTask();
        context.getElectionConfirmQueue().updateTerm(term);
        commonConfig.getParticipants()
                .parallelStream()
                .filter(e -> !e.isOntology())
                .forEach(
                        participant -> {
                            try {
                                Future<RemoteCalls.ElectResp> respFuture = stub.elect(participant.getEndpoint(), term);
                                long begin = System.currentTimeMillis();
                                while (!respFuture.isDone() && System.currentTimeMillis() - begin < commonConfig.getElectTimeout()) {
                                    // ignore it
                                }
                                if (respFuture.isDone()) {
                                    try {
                                        RemoteCalls.ElectResp resp = respFuture.get();
                                        if (resp.getAuthorized()
                                                && term == resp.getTerm()) {
                                            context.getElectionConfirmQueue().addSignalIfNx(participant.getEndpoint(), term);
                                        }
                                    } catch (InterruptedException | ExecutionException e) {
                                        log.error("electing itself response failed");
                                    }
                                }
                            } catch (Exception e) {
                                log.error("broadcast electing message failed", e);
                            }
                        }
                );
    }

    /**
     * starting a sleep timeout task
     */
    public void startSleepTimeoutTask() {
        asyncExecutor.execute(new SleepTimeoutTask());
    }

    /**
     * starting a heartbeat task
     */
    public void startHeartbeatTask() {
        context.setHeartbeatOn(true);
        asyncExecutor.execute(new HeartbeatTask());
    }

    /**
     * canceling heartbeat task
     */
    public void cancelHeartbeatTask() {
        context.setHeartbeatOn(false);
    }

    /**
     * starting a election confirming task
     */
    public void startElectionConfirmingTask() {
        asyncExecutor.execute(new ElectionConfirmingTask());
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
     * acquire for leader information
     *
     * if the leader itself is current node, alter current state into ELECTED
     *
     * @return leader
     */
    @Nullable
    public Participant acquireLeader() {
        Participant leader = null;
        int quorum = context.getQuorum(commonConfig.getParticipants().size());
        List<Participant> sub = commonConfig.getParticipants().stream().filter(e -> !e.isOntology()).collect(Collectors.toList()).subList(0, quorum);
        for (Participant p : sub) {
            Future<RemoteCalls.AcquireLeaderResp> respFuture = stub.acquireLeader(p.getEndpoint());
            long start = System.currentTimeMillis();
            while (!respFuture.isDone() && System.currentTimeMillis() - start < commonConfig.getAcquireLeaderTimeout()) {
                // spin
            }
            if (!respFuture.isDone()) {
                continue;
            }
            try {
                RemoteCalls.AcquireLeaderResp resp = respFuture.get();
                RemoteCalls.BindAddress address = resp.getLeaderAddress();
                Participant tmp = Participant.from(address, false);
                if (commonConfig.isValidParticipant(tmp)) {
                    leader = tmp;
                    state = NodeState.ELECTED;
                    term.set(resp.getTerm());
                }
                break;
            } catch (InterruptedException | ExecutionException e) {
                log.error("something's wrong with acquiring leader");
            }
        }
        return leader;
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
                    if (context.isAuthorizable(termVal)) {
                        context.setTermAcked(termVal);
                    }
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
                            try {
                                // vote for itself
                                context.getPreVoteConfirmQueue().addSignalIfNx(commonConfig.getBindEndpoint(), termVal);
                                Future<RemoteCalls.PreVoteAck> future = stub.preVote(endpoint, termVal);
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
                                if (future.isDone()) {
                                    RemoteCalls.PreVoteAck ack = future.get();
                                    log.info("PreVote acknowledge endpoint={},content={}", endpoint.toString(), ack.toString());
                                    if (ack.getAuthorized()) {
                                        context.getPreVoteConfirmQueue().addSignalIfNx(endpoint, termVal);
                                    }
                                } else {
                                    log.info("it's timeout for waiting response");
                                }
                            } catch (Exception e) {
                                log.error("There're some problems when retrieving result from the preVoteAck, endpoint={}", endpoint.toString());
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
            long currTerm = currTerm();
            int quorum = context.getQuorum(commonConfig.getParticipants().size());
            while (System.currentTimeMillis() - begin < commonConfig.getPreVoteConfirmTimeout()) {
                int size = context.getPreVoteConfirmQueue().size();
                if (currTerm == term && size >= quorum) {
                    log.info("current term={}, confirm queue size={}", currTerm, size);
                    electItselfNode(currTerm);
                    return;
                }
                // sleep for a second
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("PreVoteConfirmingTask was interrupted");
                }
            }
            log.warn("PreVoteConfirmingTask timeout in {} ms", commonConfig.getPreVoteConfirmTimeout());
            startSleepTimeoutTask();
        }
    }

    /**
     * confirm check for election stage
     */
    public class ElectionConfirmingTask implements Runnable {

        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            int quorum = context.getQuorum(commonConfig.getParticipants().size());
            while (System.currentTimeMillis() - begin < commonConfig.getElectConfirmTimeout()) {
                int size = context.getElectionConfirmQueue().size();
                if (size >= quorum) {
                    log.info("election confirm queue size={}", size);
                    state = NodeState.ELECTED;
                    leader = Participant.from(commonConfig.getBindEndpoint(), true);
                    // starting heartbeat task
                    startHeartbeatTask();
                    return;
                }
                // sleep for a second
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("ElectionConfirmingTask was interrupted");
                }
            }
            log.warn("ElectionConfirmingTask timeout in {} ms", commonConfig.getElectConfirmTimeout());
            startSleepTimeoutTask();
        }
    }

    public class HeartbeatTask implements Runnable {

        private final int timeout = 5000;

        @Override
        public void run() {
            while (context.isHeartbeatOn()) {
                long begin = System.currentTimeMillis();
                commonConfig.getParticipants().parallelStream().filter(e -> !e.isOntology()).forEach(
                        p -> {
                            Future<RemoteCalls.HeartbeatAck> ackFuture = stub.heartbeat(p.getEndpoint());
                            while (!ackFuture.isDone() && System.currentTimeMillis() - begin < timeout) {
                                // spin
                            }
                            if (ackFuture.isDone()) {
                                try {
                                    ackFuture.get();
                                } catch (InterruptedException | ExecutionException e) {
                                    log.error("Heartbeat future get failed ,endpoint={}", p.getEndpoint().toString());
                                }
                            } else {
                                replicatorManager.removeReplicator(p.getEndpoint());
                                log.error("Session timeout in {} ms, endpoint={}, this replicator would be removed by replicator manager", timeout, p.getEndpoint().toString());
                            }
                        }
                );
                try {
                    Thread.sleep(commonConfig.getHeartbeatInterval());
                } catch (InterruptedException e) {
                    log.error("HeartbeatTask sleeping thread was interrupted");
                }
            }
        }
    }
}
