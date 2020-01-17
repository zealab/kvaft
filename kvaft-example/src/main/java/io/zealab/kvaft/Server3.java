package io.zealab.kvaft;

import io.zealab.kvaft.rpc.NioServer;

public class Server3 {

    public static void main(String[] args) {
        NioServer server = new NioServer(8082);
        server.init();
        server.start();
    }
}
