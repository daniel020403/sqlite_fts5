package com.local.dev.db.sqlite;

public class MailDataState extends Mail {

    private long fileId;
    private int needUpdate;

    public MailDataState(Long fileId, String sender, String recipient, String message, int needUpdate) {
        super(sender, recipient, message);
        this.fileId     = fileId;
        this.needUpdate = needUpdate;
    }

    public long getFileId() { return this.fileId; }

}
