package com.local.dev.db.sqlite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InsertTable implements Runnable {

    private Thread thread;
    private int sampleCount;

    private static String dbFile;
    private static String ftsTable                                  = "ftsMail";
    private static String persistentTable                           = "file";
    private static String message500KB                              = "message_5k_bytes.txt";
    private static HashMap<String, Duration> threadTimedEvents      = new HashMap<String, Duration>();

    public InsertTable(String db, int dataCount) {
        System.out.println("Instantiating thread ...");
        this.thread         = new Thread(this);
        this.dbFile         = db;
        this.sampleCount    = dataCount;
    }

    public void start() {
        System.out.println("Starting thread ...");
        this.thread.start();
    }

    public void run() {
        System.out.println("Running thread ...");
        for (int i = 0; i < this.sampleCount; i++) {
            insertTestData(i);
        }
    }

    private static void insertTestData(int ptr) {
        Instant start                   = Instant.now();
        SQLiteStore sqLiteStore         = new SQLiteStore(new File(dbFile));
        String sentence                 = "";

        try {
            sentence = new String(Files.readAllBytes(Paths.get(message500KB)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, String> email   = new HashMap<String, String>();
        email.put("mail_from", String.format("user%d@email.com", ptr));
        email.put("mail_to", String.format("user%d@email.com", ptr + 1));
        email.put("mail_content", sentence);
        sqLiteStore.insertData(persistentTable, email);

        Instant end = Instant.now();
        threadTimedEvents.put("insertTestData", Duration.between(start, end));
    }

}
