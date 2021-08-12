package com.local.dev.db.sqlite;

import java.sql.*;

public class MailDataToIndex extends Mail {

    private long mailId;
    private int mailIndexed;

    public MailDataToIndex(Long id, String sender, String recipient, String message, int indexed) {
        super(sender, recipient, message);
        this.mailId         = id;
        this.mailIndexed    = indexed;
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
