package com.local.dev.db.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public class MailData extends Mail {

    public MailData(String sender, String recipient, String message) {
        super(sender, recipient, message);
    }

    @Override
    public void storeData(Connection datastoreConnection, String table) {
        Instant t1 = Instant.now();
        try {
            if (datastoreConnection != null) {
                String sql = "INSERT INTO " + table + "(backup_in_job, parent_id, name, backup_by_job, mail_from, mail_to, mail_content) " +
                        "VALUES(20210630154250, 1, '$JOB_ATTRIBUTES', 20210630154250, ?, ?, ?);";

                PreparedStatement pstatement = datastoreConnection.prepareStatement(sql);
                pstatement.setString(1, this.getSender());
                pstatement.setString(2, this.getRecipient());
                pstatement.setString(3, this.getMessage());
                pstatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Instant t2 = Instant.now();
            System.out.println("Insert data took " + Duration.between(t1, t2).toMillis());
        }
    }
}
