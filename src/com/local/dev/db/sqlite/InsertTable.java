package com.local.dev.db.sqlite;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class InsertTable implements Runnable {

    private Semaphore mutex = new Semaphore(1);

    private Thread thread;
    private String threadName;
    private String dbFile;
    private String ftsTable                                  = "ftsMail";
    private String persistentTable                           = "file";
    private long samplePtr;
    private String content;
    private HashMap<String, Duration> threadTimedEvents;

    public InsertTable(String name, String db, HashMap<String, Duration> timedEvents) {
//        System.out.println("Instantiating " + name + " ...\n");
        System.out.println("Start of insert operation: " + Instant.now());
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.dbFile             = db;
        this.threadTimedEvents  = new HashMap<>();
    }

    public void start(String content, long ptr) {
//        System.out.println("Starting " + this.threadName + " ...\n");
        this.content    = content;
        this.samplePtr  = ptr;
        this.thread.start();
    }

    public void run() {
//        System.out.println("Running " + this.threadName + " ...\n");
//        Instant start = Instant.now();
        insertTestData();
        indexData();
//        Instant end = Instant.now();
//        threadTimedEvents.put(threadName + ".runNanos", Duration.between(start, end));

//        System.out.println("Stopping " + this.threadName + " ...\n");

//        printThreadTimedEvents();
    }

    private void insertTestData() {
//        Instant start                   = Instant.now();
        SQLiteStore sqLiteStore         = new SQLiteStore(new File(dbFile));

        HashMap<String, String> email   = new HashMap<>();
        email.put("mail_from", String.format("user%d@email.com", this.samplePtr));
        email.put("mail_to", String.format("user%d@email.com", this.samplePtr + 1));
        email.put("mail_content", this.content);
        sqLiteStore.insertData(persistentTable, email);

//        Instant end = Instant.now();
//        threadTimedEvents.put(threadName + ".insertDataNanos", Duration.between(start, end));
    }

    private void indexData() {
        try {
            mutex.acquire();
            IndexData thread = new IndexData(this.threadName + ".index", this.dbFile);
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mutex.release();
        }
    }

    private void printThreadTimedEvents() {
//        long readMessageFileTotalNanos  = 0;
//        long insertDataTotalNanos       = 0;

        System.out.println("\n\n----- " + this.threadName + " Summary of Thread Timed Events (milliseconds) -----\n");
        for (Map.Entry<String, Duration> set : threadTimedEvents.entrySet()) {
//            if (set.getKey().contains("readMessageFile")) {
//                readMessageFileTotalNanos += set.getValue().toNanos();
//            } else if (set.getKey().contains("insertData")) {
//                insertDataTotalNanos += set.getValue().toNanos();
//            } else {
                System.out.println(set.getKey() + ": " + set.getValue().toMillis());
//            }
        }

//        System.out.println("readMessageFileTotalNanos: " + readMessageFileTotalNanos);
//        System.out.println("insertDataTotalNanos: " + insertDataTotalNanos);
    }

}
