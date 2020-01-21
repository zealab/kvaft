package io.zealab.kvaft;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Boot {

    public static void main(String[] args) throws InterruptedException {
        // NioServer nettyServer = new NioServer(8080);
        // nettyServer.init();
        // nettyServer.start();

        Thread.currentThread().join();
    }
}
