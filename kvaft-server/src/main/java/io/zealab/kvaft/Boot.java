package io.zealab.kvaft;

import io.zealab.kvaft.rpc.NettyServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Boot {

    public static void main(String[] args) throws InterruptedException {
        NettyServer nettyServer = new NettyServer(8080);
        nettyServer.init();
        nettyServer.start();
        Thread.currentThread().join();
    }
}
