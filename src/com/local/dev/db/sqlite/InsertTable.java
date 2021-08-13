package com.local.dev.db.sqlite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public class InsertTable extends MailIndexer implements Runnable, MailIndexingThread {

    private Thread thread;
    private String threadName;
    private int dataCount;
    private int messageSizeInBytes;

    private String persistentTable      = "file";

    public InsertTable(SQLiteFTSTest.Flags flags, String name, Connection conn) {
        System.out.println("[" + name + "] Start of thread for insert operation: " + Instant.now());
        this.flags = flags;
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.connection         = conn;
    }

    public void start(int sampleData, int contentSizeInBytes) {
        this.dataCount          = sampleData;
        this.messageSizeInBytes = contentSizeInBytes;
        flags.setInsertDone(false);
        flags.setInsertLast(false);
        this.thread.start();
    }

    public void join() throws InterruptedException {
        this.thread.join();
    }

    public boolean isAlive() {
        return this.thread.isAlive();
    }

    public void run() {
        processData();
        System.out.println("[" + this.threadName + "] End of thread for insert operations: " + Instant.now());
    }

//    public void clearTable() {
//        try (Connection connection = DriverManager.getConnection(this.connectionString)) {
////            FILE_TABLE_LOCK.acquire();
//            Mail.clearTable(connection, this.persistentTable);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
////            FILE_TABLE_LOCK.release();
//            flags.setInsertDone(true);
//        }
//    }

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
        Instant t1 = Instant.now();
        for (int i = 0; i < this.dataCount; i++) {
            MailData email = new MailData("sender" + i + "@email.com", "recipient" + i + "@email.com", message);

            try {
//                FILE_TABLE_LOCK.acquire();
                email.storeData(this.connection, this.persistentTable);
                this.connection.commit();
            } catch (Exception e) {
                e.printStackTrace();
                try { this.connection.rollback(); }
                catch (SQLException err) { err.printStackTrace(); }
            } finally {
//                FILE_TABLE_LOCK.release();
                flags.setInsertDone(true);
            }
        }
        Instant t2 = Instant.now();
        System.out.println("Insert all data took " + Duration.between(t1, t2).toMillis());
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
        flags.setInsertLast(true);
//        while (!this.indexDone) {
//            try {
//                Thread.sleep(5000);
//                break;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }

}
