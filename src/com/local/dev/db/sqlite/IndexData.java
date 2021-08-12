package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class IndexData extends MailIndexer implements Runnable, MailIndexingThread {

    private Thread thread;
    private String threadName;
    private ArrayList<MailDataToIndex> data;

    private String persistentTable  = "file";
    private String fts5Table        = "ftsMail";
    private Instant t1;
    private Instant t2;

    public IndexData(SQLiteFTSTest.Flags flags, String name, Connection conn) {
        System.out.println("[" + name + "] Start of thread for index operation: " + Instant.now());
        this.flags = flags;
        this.thread             = new Thread(this);
        this.threadName         = name;
        this.connection         = conn;
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
        try {
            if (this.connection != null) {
                String sql = "INSERT INTO " + this.fts5Table + "(rowid, mail_from, mail_to, mail_content) VALUES(?, ?, ?, ?);";
                PreparedStatement pstatement = this.connection.prepareStatement(sql);

                for (MailDataToIndex entry : this.data) {
                    pstatement.setLong(1, entry.getMailId());
                    pstatement.setString(2, entry.getSender());
                    pstatement.setString(3, entry.getRecipient());
                    pstatement.setString(4, entry.getMessage());
                    pstatement.addBatch();
                }

                pstatement.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        System.out.println("[" + this.threadName + "] End of index processData: " + Instant.now());
    }

    private ArrayList<MailDataToIndex> retrieveMailDataToIndex() {
        ArrayList<MailDataToIndex> dataset = new ArrayList<>();
        try {
//            FILE_TABLE_LOCK.acquire();
            dataset     = (new SQLiteStore(new File("index.db"))).getMailDataToIndex(this.connection, this.persistentTable);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            FILE_TABLE_LOCK.release();
        }

        return dataset;
    }

    private void flagIndexedMailData() {
//        System.out.println("[" + this.threadName + "] Start of index flagIndexedMailData: " + Instant.now());
        for (MailDataToIndex entry : this.data) {
            try {
//                FILE_TABLE_LOCK.acquire();
                entry.setMailIndexed(1);
                entry.updateData(this.connection, this.persistentTable);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
//                FILE_TABLE_LOCK.release();
            }
        }
//        System.out.println("[" + this.threadName + "] End of index flagIndexedMailData: " + Instant.now());
    }
}
