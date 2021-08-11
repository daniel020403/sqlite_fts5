package com.local.dev.db.sqlite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;

public class InsertTable extends MailIndexer implements Runnable, MailIndexingThread {

    private Thread thread;
    private String threadName;
    private String dbFile;
    private String connectionString;
    private int dataCount;
    private int messageSizeInBytes;

    private String persistentTable      = "file";

    public InsertTable(String name, String db) {
        System.out.println("[" + name + "] Start of thread for insert operation: " + Instant.now());
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.dbFile             = db;
        this.connectionString   = "jdbc:sqlite:" + this.dbFile;
    }

    public void start(int sampleData, int contentSizeInBytes) {
        this.dataCount          = sampleData;
        this.messageSizeInBytes = contentSizeInBytes;
        this.insertDone         = false;
        this.thread.start();
    }

    public void run() {
        processData();
        System.out.println("[" + this.threadName + "] End of thread for insert operations: " + Instant.now());
    }

    public void processData() {
        /***
         * Retrieval of mail data is still subject to changes
         * depending  on the knowledge that is acquired upon
         * tracing backup process.
         *
         * data assumptions:
         * - sender     > sender<i>@email.com
         * - recipient  > recipient<i>@email.com
         * - message    > message
         */
        String message          = getMessageFromFile();
        System.out.println("[" + this.threadName + "] Start of insert operations: " + Instant.now());
        for (int i = 0; i < this.dataCount; i++) {
            MailData email = new MailData("sender" + i + "@email.com", "recipient" + i + "@email.com", message);

            try (Connection connection = DriverManager.getConnection(this.connectionString)) {
                FILE_TABLE_LOCK.acquire();
                email.storeData(connection, this.persistentTable);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                FILE_TABLE_LOCK.release();
            }
        }

        endProcess();
    }

    private String getMessageFromFile() {
        String message = "";

        try {
            switch (this.messageSizeInBytes) {
                case 1000:
                    message = new String(Files.readAllBytes(Paths.get("message_1k_bytes.txt")));
                    break;
                case 500000:
                    message = new String(Files.readAllBytes(Paths.get("message_500k_bytes.txt")));
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return message;
    }

    private void endProcess() {
        this.insertDone = true;
        while (!this.indexDone) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
