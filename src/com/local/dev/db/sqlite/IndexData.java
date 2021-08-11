package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class IndexData extends MailIndexer implements Runnable, MailIndexingThread {

    private Thread thread;
    private String threadName;
    private String dbFile;
    private String connectionString;
    private ArrayList<MailDataToIndex> data;

    private String persistentTable  = "file";
    private String fts5Table        = "ftsMail";
    private Instant t1;
    private Instant t2;

    public IndexData(SQLiteFTSTest.Flags flags, String name, String db) {
        System.out.println("[" + name + "] Start of thread for index operation: " + Instant.now());
        this.flags = flags;
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.dbFile             = db;
        this.connectionString   = "jdbc:sqlite:" + this.dbFile;
    }

    public void setDaemon(boolean value) {
        this.thread.setDaemon(value);
    }

    public void start() {
        flags.setIndexDone(false);
        this.thread.start();
    }

    public void join() throws InterruptedException {
        this.thread.join();
    }

    public boolean isAlive() {
        return this.thread.isAlive();
    }

    public void run() {
        while (true) {
            if (flags.isInsertDone()) {
                if (t1 == null) t1 = Instant.now();
                this.data = retrieveMailDataToIndex();
                processData();
                flagIndexedMailData();
                flags.setInsertDone(false);
                if (flags.isInsertLast()) {
                    flags.setIndexDone(true);
                    t2 = Instant.now();
                    break;
                }
            }
        }
        System.out.println("[" + this.threadName + "] End of thread for index operation: " + Instant.now());
        System.out.println("Insert all index took " + Duration.between(t1, t2).toMillis());
    }

    public void processData() {
//        System.out.println("[" + this.threadName + "] Start of index processData: " + Instant.now() + " [" + this.data.size() + "]");
        for (MailDataToIndex entry : this.data) {
            try (Connection connection = DriverManager.getConnection(this.connectionString)) {
                FTSMAIL_TABLE_LOCK.acquire();
                entry.storeData(connection, this.fts5Table);
//                System.out.println("[" + this.threadName + "] Doing index processData: " + Instant.now());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                FTSMAIL_TABLE_LOCK.release();
            }
        }
//        System.out.println("[" + this.threadName + "] End of index processData: " + Instant.now());
    }

    private ArrayList<MailDataToIndex> retrieveMailDataToIndex() {
        ArrayList<MailDataToIndex> dataset = new ArrayList<>();
        try {
            FILE_TABLE_LOCK.acquire();
            dataset     = (new SQLiteStore(new File(this.dbFile))).getMailDataToIndex(this.persistentTable);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            FILE_TABLE_LOCK.release();
        }

        return dataset;
    }

    private void flagIndexedMailData() {
//        System.out.println("[" + this.threadName + "] Start of index flagIndexedMailData: " + Instant.now());
        for (MailDataToIndex entry : this.data) {
            try (Connection connection = DriverManager.getConnection(this.connectionString)) {
                FILE_TABLE_LOCK.acquire();
                entry.setMailIndexed(1);
                entry.updateData(connection, this.persistentTable);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                FILE_TABLE_LOCK.release();
            }
        }
//        System.out.println("[" + this.threadName + "] End of index flagIndexedMailData: " + Instant.now());
    }
}
