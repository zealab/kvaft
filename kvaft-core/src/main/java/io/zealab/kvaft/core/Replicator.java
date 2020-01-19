package io.zealab.kvaft.core;

import io.zealab.kvaft.rpc.client.Client;
import io.zealab.kvaft.rpc.client.StubImpl;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * replicator
 *
 * @author LeonWong
 */
@Slf4j
@ToString
public class Replicator {

    private Endpoint endpoint;

    private Client client;

    private volatile ReplicatorState state;

    private volatile ScheduledFuture<?> heartbeatTimer;

    private StubImpl stub = new StubImpl();

    public String nodeId() {
        return endpoint.toString();
    }

    public Replicator(Endpoint endpoint, Client client, ReplicatorState state) {
        this.endpoint = endpoint;
        this.client = client;
        this.state = state;
    }

    public synchronized void close() {
        if (this.state == ReplicatorState.CONNECTED) {
            this.state = ReplicatorState.DISCONNECTED;
            this.heartbeatTimer.cancel(true);
            this.heartbeatTimer = null;
            client.close();
        }
    }

    public Client getClient() {
        return client;
    }

    public void startHeartbeatTimer() {
        this.heartbeatTimer = TimerManager.scheduleWithFixRate(new HeartbeatTask(), 10, 10, TimeUnit.SECONDS);
    }

    /**
     * heartbeat timer task
     */
    public class HeartbeatTask implements Runnable {

        @Override
        public void run() {
            log.info("HeartbeatTask running...");
            stub.heartbeat(endpoint);
        }
    }
}
