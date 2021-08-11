package com.local.dev.db.sqlite;

import java.sql.Connection;

public abstract class Mail {

    private String sender;
    private String recipient;
    private String message;

    public Mail(String sender, String recipient, String message) {
        this.sender = sender;
        this.recipient = recipient;
        this.message = message;
    }

    public abstract void storeData(Connection datastoreConnection, String table);

    public String getSender() {
        return this.sender;
    }
    public String getRecipient() {
        return this.recipient;
    }
    public String getMessage() {
        return this.message;
    }

}
