package com.local.dev.db.sqlite;

import java.sql.Connection;

public abstract class MailIndexer {
    protected SQLiteFTSTest.Flags flags;
    protected Connection connection;
}
