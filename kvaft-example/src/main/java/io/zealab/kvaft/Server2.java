package io.zealab.kvaft;

import io.zealab.kvaft.core.NodeEngine;

public class Server2 {

    public static void main(String[] args) throws InterruptedException {
        NodeEngine engine = new NodeEngine();
        engine.init();
        engine.start();
        Thread.currentThread().join();
    }
}
