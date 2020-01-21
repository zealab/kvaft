package io.zealab.kvaft;

import io.zealab.kvaft.core.NodeEngine;

public class Server1 {

    public static void main(String[] args) throws Exception {
        NodeEngine engine = new NodeEngine();
        engine.init();
        engine.start();
        Thread.currentThread().join();
    }
}
