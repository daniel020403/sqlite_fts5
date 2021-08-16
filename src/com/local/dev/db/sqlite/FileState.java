package com.local.dev.db.sqlite;

public class FileState {

    private long stateId;
    private int state;
    private long fileId;

    public FileState(long id, int state, long fileId) {
        this.stateId    = id;
        this.state      = state;
        this.fileId     = fileId;
    }

    public long getStateId() { return this.stateId; }
    public int getState() { return this.state; }
    public long getFileId() { return this.fileId; }

    public void setState(int state) { this.state = state; }

}
