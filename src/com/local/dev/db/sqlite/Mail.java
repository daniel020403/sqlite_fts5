package com.local.dev.db.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class Mail {

    private String sender;
    private String recipient;
    private String message;

    public Mail(String sender, String recipient, String message) {
        this.sender = sender;
        this.recipient = recipient;
        this.message = message;
    }

    public String getSender() {
        return this.sender;
    }
    public String getRecipient() {
        return this.recipient;
    }
    public String getMessage() {
        return this.message;
    }

    public static void clearTable(Connection datastoreConnection, String table) {
        try {
            if (datastoreConnection != null) {
                String sql = "DELETE FROM " + table + ";";
                PreparedStatement pstatement = datastoreConnection.prepareStatement(sql);
                pstatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
