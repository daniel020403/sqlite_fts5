package com.local.dev.db.sqlite;

import java.time.Instant;

public class IndexData implements Runnable {

    private Thread thread;
    private String threadName;
    private String dbFile;

    public IndexData(String name, String db) {
        this.thread     = new Thread(this);
        this.threadName = name;
        this.dbFile     = db;
    }

    public void start() {
        this.thread.start();
    }

    public void run() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\nStopping " + this.threadName + " ...");
        System.out.println(Instant.now());
    }
}
