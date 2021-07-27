package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLiteFTSTest {

    public static void main(String[] args) {
        String sqliteDb = "index.db";
//        backupSession(sqliteDb);
        restoreSession(sqliteDb);
    }

    private static void backupSession(String db) {
        try {
            System.out.println("\nSimulating backup session!\n");
            createSQLiteDB(db);
            createTable(db);
            createFTSTable(db);
            insertTestData(db, 10);
            queryFTSTable(db);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void restoreSession(String db) {
        try {
            System.out.println("\n\n\nSimulating restore session!\n");
//            createFTSTable(db);
            queryFTSTable(db);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createSQLiteDB(String db) throws SQLException {
        File file = new File((db));
        if (!file.exists() && !file.isDirectory()) {
            SQLiteStore sqliteStore = new SQLiteStore(new File(db));
            sqliteStore.createSQLiteDB();
        } else {
            System.out.println("SQLite database is already existing: " + file.toString());
        }
    }

    private static void createTable(String db) {
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));
        String persistentTable  = "pTable";

        if (sqLiteStore.tableExist(persistentTable)) {
            System.out.println("Table " + persistentTable + " already exists!");
        } else {
            sqLiteStore.createTable(persistentTable);
            System.out.println("Table " + persistentTable + " successfully created!");
        }
    }

    private static void createFTSTable(String db) {
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));
        String ftsTable         = "fTable";
        String persistentTable  = "pTable";

        if (sqLiteStore.tableExist(ftsTable)) {
            System.out.println("FTS table " + ftsTable + " already exists!");
        } else {
            sqLiteStore.createFTSTable(ftsTable, persistentTable);
            System.out.println("FTS table " + ftsTable + " successfully created!");
        }
    }

    private static void insertTestData(String db, Integer dataCount) {
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));
        String table            = "pTable";
        String ftsTable         = "fTable";

        for (int i = 0; i < dataCount; i++) {
            sqLiteStore.insertData(table, ftsTable, i);
        }

        System.out.println("Successfully inserted " + dataCount + " test data!");
    }

    private static void queryTable(String db) throws SQLException {
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));
        String table            = "pTable";

        System.out.println("Querying table " + table + " ...");
        ResultSet resultSet     = sqLiteStore.queryTable(table);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("sender") + " - " +
                    resultSet.getString("recipient") + " - " +
                    resultSet.getString("message"));
        }
    }

    private static void queryFTSTable(String db) throws SQLException {
        SQLiteStore sqLiteStore = new SQLiteStore(new File(db));
        String ftsTable         = "fTable";

        System.out.println("Querying FTS table " + ftsTable + " ...");
        sqLiteStore.queryFTSTable(ftsTable);
    }

}
