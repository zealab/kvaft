package io.zealab.kvaft;

import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.NettyServer;

public class Boot {

    public static void main(String[] args) throws InterruptedException {
        ChannelProcessorManager manager = new ChannelProcessorManager();
        manager.init();
        NettyServer server = new NettyServer(8081);
        server.init();
        server.start();
        Thread.currentThread().join();
    }
}
