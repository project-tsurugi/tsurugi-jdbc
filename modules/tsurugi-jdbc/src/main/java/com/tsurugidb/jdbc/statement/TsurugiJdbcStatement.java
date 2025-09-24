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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.util.SqlCloser;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

public class TsurugiJdbcStatement implements Statement, HasFactory {

    protected TsurugiJdbcFactory factory;
    protected final TsurugiJdbcConnection connection;
    protected final TsurugiJdbcStatementProperties properties;

    private TsurugiJdbcResultSet executingResultSet = null;
    private ExecuteResult lowUpdateResult = null;

    private List<String> batchSqlList = null;

    private boolean poolable = false;
    private boolean closeOnCompletion = false;

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

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw getExceptionHandler().unwrapException(iface);
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
        ExecuteResult lowResult = transaction.executeAndAutoCommit(lowTransaction -> {
            return lowTransaction.executeStatement(sql).await(timeout, TimeUnit.SECONDS);
        });

        return getUpdateCount(lowResult);
    }

    protected void closeExecutingResultSet() throws SQLException {
        this.lowUpdateResult = null;

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

    protected void setLowUpdateResult(ExecuteResult result) {
        this.lowUpdateResult = result;
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
    @TsurugiJdbcNotSupported
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancel not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null; // FIXME getWarnings()
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        closeExecutingResultSet();

        PreparedStatement lowPs;
        {
            var sqlClient = connection.getLowSqlClient();
            try {
                int timeout = properties.getDefaultTimeout();
                lowPs = sqlClient.prepare(sql, List.of()).await(timeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw getExceptionHandler().sqlException("LowPreparedStatement create error", e);
            }
        }

        boolean[] needClose = { true };
        SqlCloser psCloser = () -> {
            if (needClose[0]) {
                try {
                    lowPs.close();
                } catch (Exception e) {
                    throw getExceptionHandler().sqlException("LowPreparedStatement close error", e);
                }
            }
        };

        try (psCloser) {
            var transaction = connection.getTransaction();

            if (lowPs.hasResultRecords()) {
                var rs = transaction.executeOnly(lowTransaction -> {
                    var future = lowTransaction.executeQuery(lowPs, List.of());
                    return factory.createResultSet(this, transaction, future, properties);
                });

                rs.setLowPreparedStatement(lowPs);
                needClose[0] = false;

                setExecutingResultSet(rs);
                return true;
            } else {
                int timeout = properties.getExecuteTimeout();
                ExecuteResult lowResult = transaction.executeAndAutoCommit(lowTransaction -> {
                    return lowTransaction.executeStatement(lowPs, List.of()).await(timeout, TimeUnit.SECONDS);
                });

                setLowUpdateResult(lowResult);
                return false;
            }
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.executingResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        var lowResult = this.lowUpdateResult;
        if (lowResult == null) {
            return -1;
        }

        return getUpdateCount(lowResult);
    }

    protected int getUpdateCount(@Nonnull ExecuteResult lowResult) {
        long count = 0;
        for (long c : lowResult.getCounters().values()) {
            count += c;
        }
        return (int) count;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
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
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        Objects.requireNonNull(sql);

        if (this.batchSqlList == null) {
            this.batchSqlList = new ArrayList<>();
        }

        batchSqlList.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        var sqlList = this.batchSqlList;
        if (sqlList != null) {
            sqlList.clear();
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        closeExecutingResultSet();

        var sqlList = this.batchSqlList;
        if (sqlList == null || sqlList.isEmpty()) {
            return new int[0];
        }

        int timeout = properties.getExecuteTimeout();

        var transaction = connection.getTransaction();
        int[] result = transaction.executeAndAutoCommit(lowTransaction -> {
            int[] count = new int[sqlList.size()];

            int i = 0;
            for (String sql : sqlList) {
                var er = lowTransaction.executeStatement(sql).await(timeout, TimeUnit.SECONDS);
                count[i++] = getUpdateCount(er);
            }

            return count;
        });

        clearBatch();
        return result;
    }

    @Override
    public TsurugiJdbcConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException("getMoreResults not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute not supported");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.closeOnCompletion;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws SQLException {
        this.closed = true;

        try {
            var rs = this.executingResultSet;
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Statement close error", e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }
}
