package io.zealab.kvaft;

import io.zealab.kvaft.core.Endpoint;
import io.zealab.kvaft.rpc.NioServer;
import io.zealab.kvaft.rpc.client.StubImpl;

public class Server1 {

    public static void main(String[] args) throws Exception {
        NioServer server = new NioServer(8080);
        server.init();
        server.start();
        StubImpl stub = new StubImpl();
        stub.preVoteReq(Endpoint.builder().ip("127.0.0.1").port(8081).build(), 1);
        Thread.currentThread().join();
    }
}
