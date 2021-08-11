package com.local.dev.db.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MailDataToIndex extends Mail {

    private long mailId;
    private int mailIndexed;

    public MailDataToIndex(Long id, String sender, String recipient, String message, int indexed) {
        super(sender, recipient, message);
        this.mailId         = id;
        this.mailIndexed    = indexed;
    }

    @Override
    public void storeData(Connection datastoreConnection, String table) {
        try {
            if (datastoreConnection !=  null) {
                String sql = "INSERT INTO " + table + "(rowid, mail_from, mail_to, mail_content) VALUES(?, ?, ?, ?);";

                PreparedStatement pstatement = datastoreConnection.prepareStatement(sql);
                pstatement.setLong(1, this.getMailId());
                pstatement.setString(2, this.getSender());
                pstatement.setString(3, this.getRecipient());
                pstatement.setString(4, this.getMessage());
                pstatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateData(Connection datastoreConnection, String table) {
        try {
            if (datastoreConnection != null) {
                String sql = "UPDATE " + table + " SET mail_indexed = ? WHERE id = ?";

                PreparedStatement pstatement = datastoreConnection.prepareStatement(sql);
                pstatement.setInt(1, this.mailIndexed);
                pstatement.setLong(2, this.mailId);
                pstatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long getMailId() { return this.mailId; }
    public void setMailIndexed(int value) { this.mailIndexed = value; }

}
