package io.zealab.kvaft.core;

import io.zealab.kvaft.rpc.client.Client;
import io.zealab.kvaft.rpc.client.StubImpl;
import io.zealab.kvaft.util.TimerManager;
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
            client.close();
        }
    }

    public Client getClient() {
        return client;
    }
}
