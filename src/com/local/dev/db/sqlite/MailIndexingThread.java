package com.local.dev.db.sqlite;

import java.util.concurrent.Semaphore;

public interface MailIndexingThread {

    Semaphore FILE_TABLE_LOCK       = new Semaphore(1);
    Semaphore FTSMAIL_TABLE_LOCK    = new Semaphore(1);

    void processData();

}
