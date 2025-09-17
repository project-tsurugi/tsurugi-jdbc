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
package com.tsurugidb.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;

public class TsurugiJdbcStatement implements Statement, HasFactory {

    protected TsurugiJdbcFactory factory;
    protected final TsurugiJdbcConnection connection;
    protected final TsurugiJdbcStatementProperties properties;

    private TsurugiJdbcResultSet executingResultSet = null;

    private boolean closed = false;

    @TsurugiJdbcInternal
    public TsurugiJdbcStatement(TsurugiJdbcFactory factory, TsurugiJdbcConnection connection, TsurugiJdbcStatementProperties properties) {
        this.factory = factory;
        this.connection = connection;
        this.properties = properties;
    }

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = factory;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return this.factory;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw factory.getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public TsurugiJdbcResultSet executeQuery(String sql) throws SQLException {
        closeExecutingResultSet();

        var transaction = connection.getTransaction();
        var rs = transaction.executeOnly(lowTransaction -> {
            var future = lowTransaction.executeQuery(sql);
            return factory.createResultSet(this, transaction, future, properties);
        });

        setExecutingResultSet(rs);
        return rs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        closeExecutingResultSet();

        int timeout = properties.getExecuteTimeout();

        var transaction = connection.getTransaction();
        ExecuteResult result = transaction.executeAndAutoCommit(lowTransaction -> {
            return lowTransaction.executeStatement(sql).await(timeout, TimeUnit.SECONDS);
        });

        long count = 0;
        for (long c : result.getCounters().values()) {
            count += c;
        }
        return (int) count;
    }

    protected void closeExecutingResultSet() throws SQLException {
        var rs = this.executingResultSet;
        if (rs != null) {
            this.executingResultSet = null;

            if (!rs.isClosed()) {
                rs.close();
            }
        }
    }

    protected void setExecutingResultSet(TsurugiJdbcResultSet rs) {
        this.executingResultSet = rs;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getMaxRows() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return properties.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        properties.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TsurugiJdbcConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws SQLException {
        this.closed = true;

        try (var rs = executingResultSet) {
            // close only
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }
}
