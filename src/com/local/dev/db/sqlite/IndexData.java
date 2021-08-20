package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class IndexData extends MailIndexer implements Runnable, MailIndexingThread {

    private Thread thread;
    private String threadName;
    private ArrayList<FileState> data;

    private String fileTable        = "file";
    private String fileStateTable   = "file_state";
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
                updateFileState();
                this.data = retrieveMailDataToIndex();
                processData();
                flagIndexedMailData();
                flags.setInsertDone(false);
                if (flags.isInsertLast()) {
                    updateFileState();
                    this.data = retrieveMailDataToIndex();
                    processData();
                    flagIndexedMailData();
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
                String mailSql  = "SELECT id, mail_from, mail_to, mail_content from " + this.fileTable + " WHERE id = ?;";
                PreparedStatement retrieveStatement = this.connection.prepareStatement(mailSql);

                String sql = "INSERT INTO " + this.fts5Table + "(rowid, mail_from, mail_to, mail_content) VALUES(?, ?, ?, ?);";
                PreparedStatement insertStatement = this.connection.prepareStatement(sql);

                for (FileState entry : this.data) {
                    retrieveStatement.setLong(1, entry.getFileId());
                    ResultSet mailDataSet = retrieveStatement.executeQuery();

                    if (mailDataSet.next()) {
                        insertStatement.setLong(1, mailDataSet.getLong("id"));
                        insertStatement.setString(2, mailDataSet.getString("mail_from"));
                        insertStatement.setString(3, mailDataSet.getString("mail_to"));
                        insertStatement.setString(4, mailDataSet.getString("mail_content"));
                        insertStatement.addBatch();
                    }
                }

                insertStatement.executeBatch();
                this.connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try { this.connection.rollback(); }
            catch (SQLException err) { err.printStackTrace(); }
        }
//        System.out.println("[" + this.threadName + "] End of index processData: " + Instant.now());
    }

    private void updateFileState() {
        try {
            ArrayList<MailDataState> needStateUpdateDS = (new SQLiteStore(new File("index.db"))).getMailThatNeedsStateUpdate(this.connection, this.fileTable);
            insertFileState(needStateUpdateDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertFileState(ArrayList<MailDataState> dataset) {
        try {
            (new SQLiteStore(new File("index.db"))).insertFileState(this.connection, this.fileStateTable, dataset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ArrayList<FileState> retrieveMailDataToIndex() {
        ArrayList<FileState> dataset = new ArrayList<>();
        try {
//            FILE_TABLE_LOCK.acquire();
            dataset     = (new SQLiteStore(new File("index.db"))).getMailDataToIndex(this.connection, this.fileStateTable);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            FILE_TABLE_LOCK.release();
        }

        return dataset;
    }

    private void flagIndexedMailData() {
//        System.out.println("[" + this.threadName + "] Start of index flagIndexedMailData: " + Instant.now());
        try {
            if (this.connection != null) {
                String sql = "UPDATE " + this.fileStateTable + " SET state = ? WHERE id = ?";
                PreparedStatement pstatement = this.connection.prepareStatement(sql);

                for (FileState entry : this.data) {
                    pstatement.setInt(1, 0);
                    pstatement.setLong(2, entry.getStateId());
                    pstatement.addBatch();
                }

                pstatement.executeBatch();
                this.connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try { this.connection.rollback(); }
            catch (SQLException err) { err.printStackTrace(); }
        }
//        System.out.println("[" + this.threadName + "] End of index flagIndexedMailData: " + Instant.now());
    }
}
