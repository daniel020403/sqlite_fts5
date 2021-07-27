package com.local.dev.db.sqlite;

import java.io.File;
import java.sql.*;

public class SQLiteStore {
    private File file;
    private String connectionString;

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

                while (resultSet.next()) {
                    count = resultSet.getInt("cnt");
                }

                if (count > 0) {
                    return true;
                }
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

                sql                 = "CREATE VIRTUAL TABLE " + table + " USING FTS5(sender, recipient, message, content=" + persistentTable + ", content_rowid=id);";
                statement.execute(sql);

                sql                 = "CREATE TRIGGER " + persistentTable +"_ai AFTER INSERT ON " + persistentTable +" BEGIN\n" +
                                    "    INSERT INTO " + table + "(rowid, sender, recipient, message) VALUES(new.id, new.sender, new.recipient, new.message);\n" +
                                    "END;";
                statement.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String table, String ftsTable, Integer suffix) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                String sql          = "";
                Statement statement = conn.createStatement();

                sql                 = "INSERT INTO " + table + "(sender, recipient, message) VALUES('user" + suffix + "@email.com', 'user" + (suffix + 1) + "@email.com', 'Example message " + suffix + ".');";
                statement.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet queryTable(String table) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                System.out.println("Database connection established!");
                String sql          = "SELECT * FROM " + table + ";";
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);

                return resultSet;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void queryFTSTable(String table) {
        try (Connection conn = DriverManager.getConnection(this.connectionString)) {
            if (conn != null) {
                System.out.println("Database connection established!");
                String sql          = "SELECT * FROM " + table + " WHERE " + table + " MATCH 'email';";
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {
                    System.out.println(resultSet.getString("sender") + " - " +
                            resultSet.getString("recipient") + " - " +
                            resultSet.getString("message"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
