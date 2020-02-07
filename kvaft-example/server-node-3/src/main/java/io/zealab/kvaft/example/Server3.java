package io.zealab.kvaft.example;

import io.zealab.kvaft.core.Node;
import io.zealab.kvaft.core.NodeEngine;

public class Server3 {

    public static void main(String[] args) throws InterruptedException {
        Node node = new NodeEngine();
        node.init();
        node.start();
        Thread.currentThread().join();
    }
}
