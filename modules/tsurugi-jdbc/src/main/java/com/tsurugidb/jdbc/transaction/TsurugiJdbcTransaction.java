/*
 * Copyright 2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.jdbc.transaction;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionConfig;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.tsubakuro.sql.Transaction;

@TsurugiJdbcInternal
public class TsurugiJdbcTransaction implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(TsurugiJdbcTransaction.class.getName());

    private TsurugiJdbcFactory factory;
    private final Transaction lowTransaction;
    private final boolean autoCommit;
    private final TsurugiJdbcConnectionConfig propertes;

    private final AtomicBoolean executed = new AtomicBoolean(false);
    private boolean closed = false;

    public TsurugiJdbcTransaction(TsurugiJdbcFactory factory, Transaction lowTransaction, boolean autoCommit, TsurugiJdbcConnectionConfig propertes) {
        this.factory = factory;
        this.lowTransaction = lowTransaction;
        this.autoCommit = autoCommit;
        this.propertes = propertes;
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return factory.getExceptionHandler();
    }

    public Transaction getLowTransaction() {
        return this.lowTransaction;
    }

    public boolean isAutoCommit() {
        return this.autoCommit;
    }

    protected void checkExecuted() throws SQLException {
        boolean alreadyExecuted = this.executed.getAndSet(true);

        if (autoCommit && alreadyExecuted) {
            var e = new IllegalStateException("Transaction statement already executed");
            throw getExceptionHandler().sqlException("execute error", e);
        }
    }

    public <R> R executeOnly(TsurugiJdbcTransactionFunction<R> action) throws SQLException {
        checkExecuted();

        return execute(action);
    }

    public <R> R executeAndAutoCommit(TsurugiJdbcTransactionFunction<R> action) throws SQLException {
        checkExecuted();

        R result;
        try {
            result = execute(action);
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }

        if (autoCommit) {
            commit();
        }

        return result;
    }

    protected <R> R execute(TsurugiJdbcTransactionFunction<R> action) throws SQLException {
        try {
            R result = action.execute(lowTransaction);
            return result;
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Transaction execute error", e);
        }
    }

    public void commit() throws SQLException {
        try {
            var commitOption = propertes.getCommitOption();
            LOG.config(() -> String.format("commitOption=%s", commitOption));

            int timeout = propertes.getCommitTimeout();
            LOG.config(() -> String.format("commitTimeout=%d [seconds]", timeout));

            try {
                lowTransaction.commit(commitOption).await(timeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw getExceptionHandler().sqlException("Transaction commit error", e);
            }
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }

        close();
    }

    public void rollback() throws SQLException {
        try {
            int timeout = propertes.getRollbackTimeout();
            LOG.config(() -> String.format("rollbackTimeout=%d [seconds]", timeout));

            try {
                lowTransaction.rollback().await(timeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw getExceptionHandler().sqlException("Transaction rollback error", e);
            }
        } catch (Throwable e) {
            try {
                close();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }

        close();
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;

        try {
            lowTransaction.close();
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Transaction close error", e);
        }
    }

    public boolean isClosed() {
        return this.closed;
    }
}
