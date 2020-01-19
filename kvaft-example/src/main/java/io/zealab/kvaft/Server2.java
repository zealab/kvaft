package io.zealab.kvaft;

import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.NioServer;

public class Server2 {

    public static void main(String[] args) throws InterruptedException {
        GlobalScanner initializer = new GlobalScanner();
        initializer.init();
        NioServer server = new NioServer(8081);
        server.init();
        server.start();
        Thread.currentThread().join();
    }
}
