package io.zealab.kvaft.core;

import io.zealab.kvaft.rpc.client.Client;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

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

    public String nodeId() {
        return endpoint.toString();
    }

    public Replicator(Endpoint endpoint, Client client) {
        this.endpoint = endpoint;
        this.client = client;
    }

    public synchronized void close() {
        if (null != client){
            client.close();
        }
    }

    public Client getClient() {
        return client;
    }
}
