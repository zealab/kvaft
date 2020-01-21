package io.zealab.kvaft;

import io.zealab.kvaft.config.GlobalScanner;

public class Server2 {

    public static void main(String[] args) throws InterruptedException {
        GlobalScanner initializer = new GlobalScanner();
        initializer.init();
        Thread.currentThread().join();
    }
}
