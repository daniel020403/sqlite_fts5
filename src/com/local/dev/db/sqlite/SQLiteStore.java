package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class SQLiteStore {
    private File file;
    private String connectionString;

    private String ftsTable     = "ftsMail";

    public SQLiteStore(File file) {
        this.file               = file;
        this.connectionString   = "jdbc:sqlite:" + this.file;
    }

    public void createSQLiteDB() throws SQLException {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("SQLite database created: " + this.file.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean tableExist(String table) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                String sql          = "SELECT  EXISTS(SELECT name FROM sqlite_master WHERE name = '" + table + "') as cnt;";
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                Integer count       = 0;

                while (resultSet.next()) { count = resultSet.getInt("cnt"); }
                if (count > 0) { return true; }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public void createTable(String table) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                String sql          = "CREATE TABLE IF NOT EXISTS " + table + " (id INTEGER PRIMARY KEY, sender, recipient, message);";
                Statement statement = conn.createStatement();
                statement.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createFTSTable(String table, String persistentTable) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                String sql = "";
                Statement statement = conn.createStatement();

                sql                 = "CREATE VIRTUAL TABLE " + table + " USING FTS5(mail_from, mail_to, mail_content, content=" + persistentTable + ", content_rowid=id);";
                statement.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String table, HashMap<String, String> email) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                String sql = "INSERT INTO " + table + "(backup_in_job, parent_id, name, backup_by_job, mail_from, mail_to, mail_content) " +
                                "VALUES(20210630154250, 1, '$JOB_ATTRIBUTES', 20210630154250, ?, ?, ?);";

                PreparedStatement pstatement = conn.prepareStatement(sql);
                pstatement.setString(1, email.get("mail_from"));
                pstatement.setString(2, email.get("mail_to"));
                pstatement.setString(3, email.get("mail_content"));
                pstatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<FileState> getMailDataToIndex(Connection conn, String table) {
        ArrayList<FileState> dataset  = new ArrayList<>();
        String sql                          = "SELECT id, state, file_id FROM " + table + " WHERE state <> 0 AND state <> 3";

        try {
            if (conn != null) {
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {
                    FileState data = new FileState(
                            resultSet.getLong("id"),
                            resultSet.getInt("state"),
                            resultSet.getLong("file_id")
                    );

                    dataset.add(data);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return dataset;
    }

    public void updateMailIndexed(FileState data) {

    }

    public void queryFTSTable(String table) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                System.out.println("Database connection established!");
                String sql          = "SELECT * FROM " + table + " WHERE " + table + " MATCH 'email';";
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {
                    System.out.println(resultSet.getString("mail_from") + " - " +
                            resultSet.getString("mail_to") + " - " +
                            resultSet.getString("mail_content"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
