package com.local.dev.db.sqlite;

public class MailDataToIndex extends Mail {

    private long mailId;
    private int mailIndexed;

    public MailDataToIndex(Long id, String sender, String recipient, String message, int indexed) {
        super(sender, recipient, message);
        this.mailId         = id;
        this.mailIndexed    = indexed;
    }

    public long getMailId() { return this.mailId; }
    public void setMailIndexed(int value) { this.mailIndexed = value; }

}
