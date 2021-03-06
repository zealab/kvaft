package io.zealab.kvaft.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.zealab.kvaft.config.CommonConfig;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.NioServer;
import io.zealab.kvaft.rpc.client.*;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import io.zealab.kvaft.util.Assert;
import io.zealab.kvaft.util.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
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

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final NodeContext context = new NodeContext();

    private final ChannelProcessorManager cpm = ChannelProcessorManager.getInstance();

    private final ReplicatorManager rm = ReplicatorManager.getInstance();

    private final ExecutorService asyncExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2,
            Runtime.getRuntime().availableProcessors() * 2,
            30 * 10, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2 << 16),
            new ThreadFactoryBuilder().setNameFormat("node-engine-async-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

    private final ScheduledExecutorService sleepTimeoutScheduler = Executors.newScheduledThreadPool(1);


    @Override
    public boolean isLeader() {
        Participant leader = leader();
        return leader != null && leader.isOntology();
    }

    @Override
    public Participant self() {
        return Participant.from(commonConfig.getBindEndpoint(), true);
    }

    @Override
    public Long currTerm() {
        return term.get();
    }

    @Override
    public void start() {
        server.start();
        Pair<Participant, Long> leader = acquireLeader();
        Assert.notNull(leader, "leader must not be null");
        if (leader.getLeft() != null) {
            assignLeader(leader.getLeft(), leader.getRight());
        }
        scheduleSleepTimeoutTask();
    }

    @Override
    public void shutdown() {
        // TODO
    }

    @Override
    public Participant leader() {
        Lock rLock = lock.readLock();
        try {
            rLock.lock();
            return leader;
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void assignLeader(Participant candidate, Long term) {
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            if (null != term) {
                // as a follower
                if (!updateLastTerm(term)) {
                    log.error("could not assign leader=[{}] in this term=[{}],cause someone had occupied in this term", candidate.toString(), term);
                    return;
                }
                startLeaderReplicatorCheckTask(term);
                updateTerm(term, NodeState.FOLLOWING);
            } else {
                // as a leader
                this.state = NodeState.ELECTED;
            }
            this.leader = candidate;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public void handlePreVoteRequest(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        boolean authorized = false;
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            boolean doAuthorize = isTermValid(offerTerm) && updateLastTerm(offerTerm);
            if (doAuthorize) {
                updateTerm(offerTerm, NodeState.ELECTING);
                context.turnOffSleepTimeout();
                authorized = true;
            }
        } finally {
            wLock.unlock();
        }
        RemoteCalls.PreVoteAck preVoteAck = RemoteCalls.PreVoteAck.newBuilder().setTerm(offerTerm).setAuthorized(authorized).build();
        KvaftMessage<RemoteCalls.PreVoteAck> kvaftMessage = KvaftMessage.<RemoteCalls.PreVoteAck>builder().payload(preVoteAck).requestId(requestId).build();
        channel.writeAndFlush(kvaftMessage);
    }

    @Override
    public void handleElectRequest(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        boolean authorized = false;
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            boolean doAuthorize = isTermValid(offerTerm) && ensureState(NodeState.ELECTING);
            if (doAuthorize) {
                Participant p = Participant.from(peer.getEndpoint(), false);
                assignLeader(p, offerTerm);
                authorized = true;
            }
        } finally {
            wLock.unlock();
        }
        RemoteCalls.ElectResp electResp = RemoteCalls.ElectResp.newBuilder().setTerm(offerTerm).setAuthorized(authorized).build();
        KvaftMessage<RemoteCalls.ElectResp> kvaftMessage = KvaftMessage.<RemoteCalls.ElectResp>builder().payload(electResp).requestId(requestId).build();
        channel.writeAndFlush(kvaftMessage);
    }

    @Override
    public void handleHeartbeat(Peer peer, long requestId, long offerTerm) {
        Channel channel = peer.getChannel();
        Lock rLock = lock.readLock();
        boolean shouldResponse;
        try {
            rLock.lock();
            // atomic read
            shouldResponse = offerTerm == term.get() && ensureState(NodeState.ELECTED);
        } finally {
            rLock.unlock();
        }
        if (shouldResponse) {
            RemoteCalls.HeartbeatAck heartbeatAck = RemoteCalls.HeartbeatAck.newBuilder().setTimestamp(System.currentTimeMillis()).build();
            KvaftMessage<RemoteCalls.HeartbeatAck> kvaftMessage = KvaftMessage.<RemoteCalls.HeartbeatAck>builder().payload(heartbeatAck).requestId(requestId).build();
            channel.writeAndFlush(kvaftMessage);
        }
    }

    @Override
    public void handleLeaderAcquire(Peer peer, long requestId) {
        Channel channel = peer.getChannel();
        Lock rLock = lock.readLock();
        try {
            rLock.lock();
            Participant leader = leader();
            if (ensureState(NodeState.ELECTED)) {
                Long term = currTerm();
                RemoteCalls.BindAddress address = RemoteCalls.BindAddress.newBuilder()
                        .setHost(leader.getEndpoint().getIp())
                        .setPort(leader.getEndpoint().getPort())
                        .build();
                RemoteCalls.AcquireLeaderResp resp = RemoteCalls.AcquireLeaderResp.newBuilder()
                        .setLeaderAddress(address)
                        .setIsOntology(leader.isOntology())
                        .setTerm(term)
                        .build();
                KvaftMessage<RemoteCalls.AcquireLeaderResp> kvaftMessage = KvaftMessage.<RemoteCalls.AcquireLeaderResp>builder().payload(resp).requestId(requestId).build();
                channel.writeAndFlush(kvaftMessage);
            }
        } finally {
            rLock.unlock();
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
        this.cpm.bindNode(this);
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
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            if (ensureNotState(NodeState.FOLLOWING)) {
                // interrupt electing...
                log.info("interrupt the electing...");
                return;
            }
            this.context.turnOffSleepTimeout();
            this.state = NodeState.ELECTING;
            this.context.resetElectionConfirmQueue(term);
            // starting election confirming task.
            startElectionConfirmingTask(term);
        } finally {
            wLock.unlock();
        }
        // broadcast electing message
        broadcastElectingMsg(term);
    }


    /**
     * starting a sleep timeout task
     */
    public void scheduleSleepTimeoutTask() {
        context.turnOnSleepTimeout();
        sleepTimeoutScheduler.scheduleAtFixedRate(new SleepTimeoutTask(), 5, 5, TimeUnit.SECONDS);
    }

    /**
     * starting a heartbeat task
     */
    public void startHeartbeatTask(long currTerm) {
        context.turnOnHeartbeat();
        asyncExecutor.execute(new HeartbeatTask(currTerm));
    }

    public boolean updateLastTerm(long offerTerm) {
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            if (context.isAuthorizable(offerTerm)) {
                context.setLastTerm(offerTerm);
                return true;
            } else {
                return false;
            }
        } finally {
            wLock.unlock();
        }
    }

    /**
     * starting a heartbeat check task
     */
    public void startHeartbeatCheckTask(long currTerm) {
        HeartbeatCheckTask cmd = new HeartbeatCheckTask(currTerm);
        cmd.scheduleAtFixRate();
    }

    public void startLeaderReplicatorCheckTask(long currTerm) {
        LeaderReplicatorCheckTask cmd = new LeaderReplicatorCheckTask(currTerm);
        cmd.scheduleAtFixRate();
    }

    /**
     * starting a election confirming task
     */
    public void startElectionConfirmingTask(long term) {
        asyncExecutor.execute(new ElectionConfirmingTask(term));
    }

    private void broadcastPreVoteMsg(long termVal) {
        log.info("start to broadcast pre voting msg to the other participants");
        // not decide which node is leader
        context.resetPreVoteConfirmQueue(termVal);
        List<Participant> participants = commonConfig.getParticipants();
        participants.parallelStream().filter(e -> !e.isOntology()).forEach(
                p -> {
                    Endpoint endpoint = p.getEndpoint();
                    try {
                        // vote for itself
                        context.addPreVoteConfirmNx(commonConfig.getBindEndpoint(), termVal);
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
                                context.addPreVoteConfirmNx(endpoint, termVal);
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

    private void broadcastElectingMsg(long termVal) {
        commonConfig.getParticipants()
                .parallelStream()
                .filter(e -> !e.isOntology())
                .forEach(
                        participant -> {
                            try {
                                Future<RemoteCalls.ElectResp> respFuture = stub.elect(participant.getEndpoint(), termVal);
                                long begin = System.currentTimeMillis();
                                while (!respFuture.isDone() && System.currentTimeMillis() - begin < commonConfig.getElectTimeout()) {
                                    // ignore it
                                }
                                if (respFuture.isDone()) {
                                    try {
                                        RemoteCalls.ElectResp resp = respFuture.get();
                                        if (resp.getAuthorized()
                                                && termVal == resp.getTerm()) {
                                            context.addElectionConfirmNx(participant.getEndpoint(), termVal);
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
     * broadcast step-down message from leader
     *
     * @param termVal term
     */
    public void broadcastStepDownMsg(long termVal) {
        commonConfig.getParticipants()
                .parallelStream()
                .filter(e -> !e.isOntology())
                .forEach(
                        participant -> {
                            try {
                                stub.stepDown(participant.getEndpoint(), termVal);
                            } catch (Exception e) {
                                log.error("broadcast step down message failed", e);
                            }
                        }
                );
    }

    /**
     * starting a pre vote timeout task
     */
    public void startPreVoteConfirmingTask(long term) {
        asyncExecutor.execute(new PreVoteConfirmingTask(term));
    }

    public void setConfigFileLocation(String location) {
        this.configFileLocation = location;
    }

    private void resetLeader(long term) {
        Lock wLock = lock.writeLock();
        try {
            wLock.lock();
            if (term != currTerm()) {
                log.warn("There's some weird, reset leader term is not match yet.");
                return;
            }
            this.context.turnOffHeartbeat();
            // for leader
            if (ensureState(NodeState.ELECTED)) {
                // broadcast step down message
                broadcastStepDownMsg(term);
                // clear all peers
                this.cpm.clearAllPeers();

            }
            // for follower
            if (ensureState(NodeState.FOLLOWING)) {
                // clear replicator client who connects to leader;
                if (null != this.leader) {
                    this.rm.removeReplicator(leader.getEndpoint());
                }
            }
            // reset state and leader
            this.leader = null;
            this.state = NodeState.FOLLOWING;
            this.context.turnOnSleepTimeout();
        } finally {
            wLock.unlock();
        }

    }

    private boolean ensureState(NodeState state) {
        return this.state.compareTo(state) == 0;
    }

    private boolean ensureNotState(NodeState state) {
        return this.state.compareTo(state) != 0;
    }

    private boolean isTermValid(long offerTerm) {
        return currTerm() <= offerTerm;
    }

    /**
     * This method is not thread safe, it must be carefully when you use it
     *
     * @param termVal the term need to update
     * @param state   the state need to update
     */
    private void updateTerm(long termVal, NodeState state) {
        this.state = state;
        this.term.set(termVal);
    }

    private void parseConfigFile() {
        Yaml yaml = new Yaml();
        InputStream configStream;
        try {
            configStream = Files.newInputStream(Paths.get(configFileLocation));
        } catch (IOException e) {
            log.warn("could not find any file from Files.newInputStream.");
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

    private boolean shouldSleepTimeoutTaskContinue() {
        return ensureState(NodeState.FOLLOWING) && context.isSleepTimeoutTaskOn();
    }

    /**
     * acquire for leader information
     * <p>
     * if the leader itself is current node, alter current state into ELECTED
     *
     * @return leader
     */
    @Nullable
    public Pair<Participant, Long> acquireLeader() {
        final Long[] term = {currTerm()};
        Map<Participant, Long> counted = commonConfig.getParticipants()
                .parallelStream()
                .filter(e -> !e.isOntology())
                .map(
                        p -> {
                            long start = System.currentTimeMillis();
                            Future<RemoteCalls.AcquireLeaderResp> respFuture = stub.acquireLeader(p.getEndpoint());
                            while (!respFuture.isDone() && System.currentTimeMillis() - start < commonConfig.getAcquireLeaderTimeout()) {
                                // spin
                            }
                            if (!respFuture.isDone()) {
                                return null;
                            }
                            try {
                                RemoteCalls.AcquireLeaderResp resp = respFuture.get();
                                term[0] = Math.max(term[0], resp.getTerm());
                                return resp.getIsOntology() ? p : Participant.from(resp.getLeaderAddress(), false);
                            } catch (InterruptedException | ExecutionException e) {
                                log.error("something's wrong with acquiring leader");
                            }
                            return null;
                        }
                ).filter(Objects::nonNull).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        List<Participant> possibleLeaders = counted.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).collect(Collectors.toList());
        Participant leader = possibleLeaders.size() > 0 ? possibleLeaders.get(possibleLeaders.size() - 1) : null;
        return Pair.of(leader, term[0]);
    }

    /**
     * When the sleep thread task awakes, it will broadcast preVote message to every node.
     */
    public class SleepTimeoutTask implements Runnable {

        @Override
        public void run() {
            if (!shouldSleepTimeoutTaskContinue()) {
                if (log.isDebugEnabled()) {
                    log.debug("Should NOT continue executing SleepTimeoutTask");
                }
                return;
            }
            SecureRandom random = new SecureRandom();
            int r = random.nextInt(5000);
            try {
                Thread.sleep(r);
            } catch (InterruptedException e) {
                log.error("SleepTimeoutTask was interrupted", e);
            }

            long termVal;
            Lock wLock = lock.writeLock();
            try {
                wLock.lock();
                // double check node state
                if (!shouldSleepTimeoutTaskContinue()) {
                    log.info("Should NOT continue executing SleepTimeoutTask");
                    return;
                }
                termVal = term.incrementAndGet();
                boolean isOk = updateLastTerm(termVal);
                if (!isOk) {
                    log.info("Sleeping timeout task interrupted, cause this term={} was not authorized", termVal);
                    return;
                }
            } finally {
                wLock.unlock();
            }
            // starting to check pre vote acknowledges
            startPreVoteConfirmingTask(termVal);
            // broadcasting preVote message
            broadcastPreVoteMsg(termVal);
        }
    }

    /**
     * PreVote acknowledges confirming
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
            int quorum = commonConfig.getQuorum();
            while (System.currentTimeMillis() - begin < commonConfig.getPreVoteConfirmTimeout()) {
                int size = context.preVoteConfirmQueueSize();
                if (currTerm == term
                        && size >= quorum
                        && ensureState(NodeState.FOLLOWING)) {
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
            context.turnOnSleepTimeout();
        }
    }

    /**
     * confirm check for election stage
     */
    public class ElectionConfirmingTask implements Runnable {

        private final long term;

        public ElectionConfirmingTask(long term) {
            this.term = term;
        }

        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            int quorum = commonConfig.getQuorum();
            while (System.currentTimeMillis() - begin < commonConfig.getElectConfirmTimeout()) {
                int size = context.electionConfirmQueueSize();
                if (size >= quorum) {
                    Lock rLock = lock.readLock();
                    try {
                        rLock.lock();
                        if (term == currTerm() && ensureState(NodeState.ELECTING)) {
                            log.info("The whole election process is complete with this term={}, and the quantity of election confirmed is {}", currTerm(), size);
                            assignLeader(self(), null);
                            // starting heartbeat task
                            startHeartbeatTask(term);
                            startHeartbeatCheckTask(term);
                        }
                    } finally {
                        rLock.unlock();
                    }
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
            context.turnOnSleepTimeout();
        }
    }

    /**
     * Heartbeat from leader to follower
     */
    public class HeartbeatTask implements Runnable {

        private final int timeout = 5000;

        private final long currTerm;

        public HeartbeatTask(long currTerm) {
            this.currTerm = currTerm;
        }

        @Override
        public void run() {
            while (context.isHeartbeatOn() && ensureState(NodeState.ELECTED)) {
                long begin = System.currentTimeMillis();
                commonConfig.getParticipants().parallelStream().filter(e -> !e.isOntology()).forEach(
                        p -> {
                            Future<RemoteCalls.HeartbeatAck> ackFuture = stub.heartbeat(p.getEndpoint(), currTerm);
                            while (!ackFuture.isDone() && System.currentTimeMillis() - begin < timeout) {
                                // spin
                            }
                            if (ackFuture.isDone()) {
                                try {
                                    ackFuture.get();
                                    Optional.ofNullable(
                                            cpm.getPeer(p.getEndpoint().toString())
                                    ).ifPresent(
                                            peer -> peer.setLastHbTime(System.currentTimeMillis())
                                    );
                                } catch (InterruptedException | ExecutionException e) {
                                    log.error("Heartbeat future get failed ,endpoint={}", p.getEndpoint().toString());
                                }
                            } else {
                                rm.removeReplicator(p.getEndpoint());
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

    /**
     * This task is for leader.
     * <p>
     * Check the peers from client if its session is out of time.
     */
    public class HeartbeatCheckTask implements Runnable {

        private volatile ScheduledFuture<?> future;

        private final ScheduledExecutorService heartbeatCheckScheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("heartbeat-check-%d", false));

        private final long term;

        public HeartbeatCheckTask(long term) {
            this.term = term;
        }

        @Override
        public void run() {
            if (ensureNotState(NodeState.ELECTED) || term != currTerm()) {
                log.warn("The leader's term could has been changed, term of this task is {}", term);
                future.cancel(false);
                return;
            }
            // Check the peers from client if its session is out of time.
            cpm.handleSessionTimeoutPeers(commonConfig.getPeersSessionTimeout());
            if (cpm.peerSize() + 1 < commonConfig.getQuorum()) {
                log.error("There is not enough peers available for current leader.");
                future.cancel(false);
                resetLeader(term);
            }
        }

        public void scheduleAtFixRate() {
            future = heartbeatCheckScheduler.scheduleAtFixedRate(this, 5, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * This task is for follower.
     * <p>
     * Check the replicator client who connects to the leader if is available
     */
    public class LeaderReplicatorCheckTask implements Runnable {

        private volatile ScheduledFuture<?> future;

        private final ScheduledExecutorService leaderReplicatorScheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("leader-replicator-check-%d", false));

        private final long term;

        public LeaderReplicatorCheckTask(long term) {
            this.term = term;
        }

        @Override
        public void run() {
            if (leader == null || term != currTerm() || ensureNotState(NodeState.FOLLOWING)) {
                log.warn("The follower's term could has been changed or leader was reset before probably ?, term of this task is {}", term);
                future.cancel(false);
                return;
            }
            Replicator leaderReplicator = rm.getReplicator(leader.getEndpoint().toString());
            if (null == leaderReplicator) {
                // Retry connecting to leader if client was broken
                Client client = ClientFactory.getOrCreate(leader.getEndpoint());
                leaderReplicator = rm.registerReplicator(leader.getEndpoint(), client);
                log.info("Retry connecting to the leader, retry result is {}", leaderReplicator == null
                        ? "failed"
                        : "success");
            }
        }

        public void scheduleAtFixRate() {
            future = leaderReplicatorScheduler.scheduleAtFixedRate(this, 5, 10, TimeUnit.SECONDS);
        }
    }
}
