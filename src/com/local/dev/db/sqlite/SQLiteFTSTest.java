package com.local.dev.db.sqlite;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SQLiteFTSTest {
    private static String sqliteDb                          = "index.db";
    private static String ftsTable                          = "ftsMail";
    private static String persistentTable                   = "file";
    private static List<String> dictionary                  = new ArrayList<String>();
    private static HashMap<String, Duration> timedEvents    = new HashMap<String, Duration>();
//    private static String message1000B                      = "message_1k_bytes.txt";
    private static String message1000B                      = "message_1k_bytes_proper_words.txt";
    private static String message500KB                      = "message_500k_bytes.txt";

    public static class Flags {
        protected volatile boolean insertDone;
        protected volatile boolean indexDone;
        protected volatile boolean insertLast;

        public boolean isInsertDone() {
            return insertDone;
        }

        public void setInsertDone(boolean insertDone) {
            this.insertDone = insertDone;
        }

        public boolean isIndexDone() {
            return indexDone;
        }

        public void setIndexDone(boolean indexDone) {
            this.indexDone = indexDone;
        }

        public boolean isInsertLast() {
            return insertLast;
        }

        public void setInsertLast(boolean insertLast) {
            this.insertLast = insertLast;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        Instant start = Instant.now();

//        dictionary = readDictionary();
//        backupSession(sqliteDb);
//        restoreSession(sqliteDb);

//        threadInsertData(10000, 1000);
        fts5ContentlessTableInputStreamTest();

        Instant end = Instant.now();
        timedEvents.put("main", Duration.between(start, end));

        printTimedEvents();
    }

    private static void backupSession(String db) {
        Instant start = Instant.now();

        try {
            System.out.println("\nSimulating backup session!\n");
            createSQLiteDB(db);
            createTable(db);
            createFTSTable(db);
            insertTestData(db, 1000, 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        timedEvents.put("backupSession", Duration.between(start, end));
    }

    private static void restoreSession(String db) {
        Instant start = Instant.now();

        try {
            System.out.println("\n\n\nSimulating restore session!\n");
            queryFTSTable(db);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        timedEvents.put("restoreSession", Duration.between(start, end));
    }

    private static void createSQLiteDB(String db) throws SQLException {
        Instant start = Instant.now();

        File file = new File(db);
        if (!file.exists() && !file.isDirectory()) {
            SQLiteStore sqliteStore = new SQLiteStore(new File(db));
            sqliteStore.createSQLiteDB();
            System.out.println("SQLite database created: " + file.toString());
        } else {
            System.out.println("SQLite database is already existing: " + file.toString());
        }

        Instant end = Instant.now();
        timedEvents.put("createSQLiteDB", Duration.between(start, end));
    }

    private static void createTable(String db) {
        Instant start           = Instant.now();
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));

        if (sqLiteStore.tableExist(persistentTable)) {
            System.out.println("Table " + persistentTable + " already exists!");
        } else {
            sqLiteStore.createTable(persistentTable);
            System.out.println("Table " + persistentTable + " successfully created!");
        }

        Instant end = Instant.now();
        timedEvents.put("createTable", Duration.between(start, end));
    }

    private static void createFTSTable(String db) {
        Instant start           = Instant.now();
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));

        if (sqLiteStore.tableExist(ftsTable)) {
            System.out.println("FTS table " + ftsTable + " already exists!");
        } else {
            sqLiteStore.createFTSTable(ftsTable, persistentTable);
            System.out.println("FTS table " + ftsTable + " successfully created!");
        }

        Instant end = Instant.now();
        timedEvents.put("createFTSTable", Duration.between(start, end));
    }

    private static void insertTestData(String db, Integer dataCount, int contentSizeInBytes) {
        Instant start                   = Instant.now();
        SQLiteStore sqLiteStore         = new SQLiteStore(new File(db));
        List<String> sentences          = new ArrayList<String>();
        if (contentSizeInBytes < 500000) {
            sentences = generateSentenceList(dataCount, contentSizeInBytes);
        } else {
            try {
                String content = new String(Files.readAllBytes(Paths.get(message500KB)));
                sentences.add(content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Start of data insert operation: " + Instant.now());
        HashMap<String, String> email   = new HashMap<String, String>();
        for (int i = 0; i < dataCount; i++) {
            email.put("mail_from", String.format("user%d@email.com", i));
            email.put("mail_to", String.format("user%d@email.com", i + 1));
            if (contentSizeInBytes < 500000)
                email.put("mail_content", sentences.get(i));
            else
                email.put("mail_content", sentences.get(0));
            sqLiteStore.insertData(persistentTable, email);

            email.clear();
        }
        System.out.println("Successfully inserted " + dataCount + " test data!");
        System.out.println(Instant.now());

        Instant end = Instant.now();
        timedEvents.put("insertTestData", Duration.between(start, end));
    }

    private static void queryFTSTable(String db) throws SQLException {
        Instant start           = Instant.now();
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));

        System.out.println("Querying FTS table " + ftsTable + " ...");
        sqLiteStore.queryFTSTable(ftsTable);

        Instant end = Instant.now();
        timedEvents.put("queryFTSTable", Duration.between(start, end));
    }

    private static List<String> readDictionary() {
        System.out.println("Reading dictionary file ...");

        Instant start       = Instant.now();
        List<String> lines  = new ArrayList<String>();

        try (Stream<String> stream = Files.lines(Paths.get("dictionary.txt"), StandardCharsets.UTF_8)) {
            stream.forEach(string -> lines.add(string));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        timedEvents.put("readDictionary", Duration.between(start, end));

        return lines;
    }

    private static List<String> generateSentenceList(int dataCount, int contentSizeInBytes) {
        System.out.println("Generating sentences for mail content ...");

        Instant start                   = Instant.now();
        SentenceGenerator generator     = new SentenceGenerator(dictionary);
        List<String> list               = generator.generateSentenceList(dataCount, contentSizeInBytes);
        Instant end                     = Instant.now();
        timedEvents.put("generateSentenceList", Duration.between(start, end));

        return list;
    }

    private static void printTimedEvents() {
        System.out.println("\n\n----- Summary of Timed Events (milliseconds) -----\n");
        for (Map.Entry<String, Duration> set : timedEvents.entrySet()) {
            System.out.println(set.getKey() + ": " + set.getValue().toMillis());
        }
    }

    private static void threadInsertData(int dataCount, int contentSizeInBytes) throws IOException, InterruptedException, SQLException {
        Instant start   = Instant.now();

        Flags flags = new Flags();
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteDb);
        connection.setAutoCommit(false);

        InsertTable threadInsert = new InsertTable(flags, "insertThread", connection);
//        threadInsert.clearTable();

        IndexData threadIndex = new IndexData(flags, "indexThread", connection);
//        threadIndex.setDaemon(true);

        threadIndex.start();
        threadInsert.start(dataCount, contentSizeInBytes);

        while (threadInsert.isAlive() || threadIndex.isAlive()) {
//            try {
//                System.out.println("Waiting for threads to end...");
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        connection.close();
//        Thread.sleep(1000);
        Instant end = Instant.now();
        timedEvents.put("threadInsertData", Duration.between(start, end));
    }

    private static String readDataFromFile(String file) throws IOException {
        Instant start   = Instant.now();
        String content  = "";

        try {
            content = new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        timedEvents.put("readDataFromFile", Duration.between(start, end));

        return content;
    }

    private static void fts5ContentlessTableInputStreamTest() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteDb);
            connection.setAutoCommit(false);

            fileTableInsert(connection);
            fts5ContentlessTableInsert(connection);

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void fileTableInsert(Connection connection) {
        try {
            if (connection != null) {
                String sql                          = "INSERT INTO file(backup_in_job, parent_id, name, backup_by_job, mail_from, mail_to, mail_content) " +
                                                    "VALUES(20210630154250, 1, '$JOB_ATTRIBUTES', 20210630154250, ?, ?, ?);";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);

                preparedStatement.setString(1, "sender@email.com");
                preparedStatement.setString(2, "recipient@email.com");
                preparedStatement.setString(3, "Far far away, behind the word mountains");
                preparedStatement.execute();

                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void fts5ContentlessTableInsert(Connection connection) {
        try {
            if (connection != null) {
                String sql                          = "INSERT INTO ftsMail(rowid, mail_from, mail_to, mail_content) VALUES(?, ?, ?, ?);";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                String fileContent                  = new String(Files.readAllBytes(Paths.get("message_1k_bytes_proper_words.txt")));

                preparedStatement.setInt(1, 1);
                preparedStatement.setString(2, "sender@email.com");
                preparedStatement.setString(3, "recipient@email.com");

                ByteArrayInputStream inputStream    = new ByteArrayInputStream(fileContent.getBytes());
                preparedStatement.setAsciiStream(4, inputStream, fileContent.getBytes().length);

                preparedStatement.executeUpdate();
                connection.commit();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

}
