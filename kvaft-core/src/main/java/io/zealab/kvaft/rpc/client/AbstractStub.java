package io.zealab.kvaft.rpc.client;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;
import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.core.RequestId;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;

@Slf4j
public abstract class AbstractStub implements Stub {

    @SuppressWarnings("unchecked")
    public <T extends Message, R extends Message> Future<R> doInvoke(Endpoint endpoint, T message) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        SettableFuture<R> result = SettableFuture.create();
        if (client == null) {
            log.warn("could not get any client from this endpoint={}", endpoint);
            return result;
        }
        KvaftMessage<T> req = KvaftMessage.<T>builder()
                .requestId(requestId.getValue())
                .payload(message)
                .build();
        client.invokeWithCallback(req, 1000, 1000, payload -> {
            try {
                R ack = (R) payload;
                result.set(ack);
            } catch (Exception e) {
                log.error("could not cast payload object to target class. payload class={}", payload.getClass());
            }
        });
        return result;
    }

    public <T extends Message> void doInvokeOneWay(Endpoint endpoint, T message, int soTimeout, int cTimeout) {
        Client client = ClientFactory.getOrCreate(endpoint);
        RequestId requestId = RequestId.create();
        if (client == null) {
            log.warn("could not get any client from this endpoint={}", endpoint);
            return;
        }
        KvaftMessage<T> req = KvaftMessage.<T>builder()
                .requestId(requestId.getValue())
                .payload(message)
                .build();
        client.invokeOneWay(req, soTimeout, cTimeout);
    }
}
