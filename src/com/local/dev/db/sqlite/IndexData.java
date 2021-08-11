package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
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

    public IndexData(String name, String db) {
        System.out.println("[" + name + "] Start of thread for index operation: " + Instant.now());
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.dbFile             = db;
        this.connectionString   = "jdbc:sqlite:" + this.dbFile;
    }

    public void setDaemon(boolean value) {
        this.thread.setDaemon(value);
    }

    public void start() {
        this.indexDone = false;
        this.thread.start();
    }

    public void run() {
        while (!this.insertDone) {
            this.data = retrieveMailDataToIndex();
            processData();
            flagIndexedMailData();
        }

        this.indexDone = true;
        System.out.println("[" + this.threadName + "] End of thread for index operation: " + Instant.now());
    }

    public void processData() {
//        System.out.println("[" + this.threadName + "] Start of index operations: " + Instant.now());
        for (MailDataToIndex entry : this.data) {
            try (Connection connection = DriverManager.getConnection(this.connectionString)) {
                FTSMAIL_TABLE_LOCK.acquire();
                entry.storeData(connection, this.fts5Table);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                FTSMAIL_TABLE_LOCK.release();
            }
        }
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
    }
}
