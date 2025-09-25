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
package com.tsurugidb.jdbc.resultset;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.util.SqlCloser;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi JDBC Result Set.
 */
@NotThreadSafe
public class TsurugiJdbcResultSet extends AbstractResultSet {

    private final TsurugiJdbcStatement ownerStatement;
    private final TsurugiJdbcTransaction transaction;
    private final TsurugiJdbcResultSetConfig config;
    private PreparedStatement lowPreparedStatement = null; // close on ResultSet.close()

    private FutureResponse<com.tsurugidb.tsubakuro.sql.ResultSet> resultSetFuture;
    private com.tsurugidb.tsubakuro.sql.ResultSet lowResultSet = null;
    private TsurugiJdbcResultSetMetaData resultSetMetaData = null;

    private TsurugiJdbcResultSetGetter[] getters;
    private Object[] values;

    private boolean isAfterLast = false;
    private boolean finished = false;

    /**
     * Creates a new instance.
     *
     * @param statement       statement
     * @param transaction     transaction
     * @param resultSetFuture future of ResultSet
     * @param config          configuration
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<com.tsurugidb.tsubakuro.sql.ResultSet> resultSetFuture,
            TsurugiJdbcResultSetConfig config) {
        super(statement);
        this.ownerStatement = statement;
        this.transaction = transaction;
        this.resultSetFuture = resultSetFuture;
        this.config = config;
    }

    /**
     * Set low PreparedStatement for dispose.
     *
     * @param lowPs low PreparedStatement
     */
    @TsurugiJdbcInternal
    public void setLowPreparedStatement(PreparedStatement lowPs) {
        this.lowPreparedStatement = lowPs;
    }

    /**
     * Get transaction.
     *
     * @return transaction
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcTransaction getTransaction() {
        return this.transaction;
    }

    /**
     * Get low ResultSet.
     *
     * @return low ResultSet
     * @throws SQLException if a database access error occurs
     */
    @TsurugiJdbcInternal
    protected com.tsurugidb.tsubakuro.sql.ResultSet getLowResultSet() throws SQLException {
        if (this.lowResultSet == null) {
            int timeout = config.getQueryTimeout();
            try {
                this.lowResultSet = resultSetFuture.await(timeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw getExceptionHandler().sqlException("LowResultSet get error", e);
            }
            this.resultSetFuture = null;

            lowResultSet.setTimeout(timeout, TimeUnit.SECONDS);
        }
        return this.lowResultSet;
    }

    /**
     * Get low ResultSet metadata.
     *
     * @param lowRs low ResultSet
     * @return low ResultSet metadata
     * @throws SQLException if a database access error occurs
     */
    @TsurugiJdbcInternal
    protected ResultSetMetadata getLowResultSetMetadata(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        try {
            return lowRs.getMetadata();
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("ResultSet getMetadata error", e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        var lowRs = getLowResultSet();
        if (nextRow(lowRs)) {
            this.currentRowNumber++;
            this.isAfterLast = false;

            initializeBuffer(lowRs);
            for (int i = 0; nextLowColumn(lowRs); i++) {
                var getter = getters[i];
                values[i] = fetchLowValue(lowRs, getter);
            }
            return true;
        } else {
            this.isAfterLast = true;

            if (transaction.isAutoCommit()) {
                close(); // commit
            }
            return false;
        }
    }

    private boolean nextRow(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        boolean hasNext = nextLowRow(lowRs);
        if (hasNext) {
            int maxRows = ownerStatement.getMaxRows();
            if (maxRows > 0) {
                return this.currentRowNumber < maxRows;
            }
            return true;
        }

        return false;
    }

    private boolean nextLowRow(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        try {
            return lowRs.nextRow();
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("ResultSet nextRow error", e);
        }
    }

    private void initializeBuffer(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        if (this.values == null) {
            var lowMetadata = getLowResultSetMetadata(lowRs);
            var lowColumnList = lowMetadata.getColumns();

            var getters = new TsurugiJdbcResultSetGetter[lowColumnList.size()];
            for (int i = 0; i < lowColumnList.size(); i++) {
                var lowColumn = lowColumnList.get(i);
                getters[i] = TsurugiJdbcResultSetGetter.of(lowColumn);
            }
            this.getters = getters;

            this.values = new Object[lowColumnList.size()];
        }
    }

    private boolean nextLowColumn(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        try {
            return lowRs.nextColumn();
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("ResultSet nextColumn error", e);
        }
    }

    private Object fetchLowValue(com.tsurugidb.tsubakuro.sql.ResultSet lowRs, TsurugiJdbcResultSetGetter getter) throws SQLException {
        try {
            return getter.fetchValue(this, lowRs);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("ResultSet fetchValue error", e);
        }
    }

    @Override
    public TsurugiJdbcResultSetMetaData getMetaData() throws SQLException {
        if (this.resultSetMetaData == null) {
            var lowRs = getLowResultSet();
            var lowRsMetadata = getLowResultSetMetadata(lowRs);
            this.resultSetMetaData = new TsurugiJdbcResultSetMetaData(this, lowRsMetadata);
        }
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object value;
        try {
            value = values[columnIndex - 1];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw getExceptionHandler().sqlException("getObject error", e);
        }

        this.wasNull = (value == null);
        return value;
    }

    @Override
    protected Map<String, Integer> createColumnIndexMap() throws SQLException {
        var rs = getLowResultSet();
        var lowMetadata = getLowResultSetMetadata(rs);
        var lowColumnList = lowMetadata.getColumns();

        var map = new HashMap<String, Integer>(lowColumnList.size());
        for (int i = 0; i < lowColumnList.size(); i++) {
            var lowColumn = lowColumnList.get(i);
            String name = lowColumn.getName();
            if (name != null) {
                map.put(name, i + 1);
            }
        }

        return map;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.currentRowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.isAfterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.currentRowNumber == 1;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("beforeFirst not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("afterLast not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("first not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("last not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("absolute not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean relative(int rows) throws SQLException {
        switch (rows) {
        case 0:
            return (this.currentRowNumber >= 1) && !this.isAfterLast;
        case 1:
            return next();
        default:
            throw new SQLFeatureNotSupportedException("relative not supported");
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("previous not supported");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Unsupported direction");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setFetchSize(int rows) throws SQLException {
        // not supported
    }

    @Override
    @TsurugiJdbcNotSupported
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public TsurugiJdbcStatement getStatement() throws SQLException {
        return this.ownerStatement;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;

        SqlCloser statement = () -> {
            if (ownerStatement.isCloseOnCompletion()) {
                ownerStatement.close();
            }
        };
        SqlCloser commit = this::finish; // commit when AutoCommit

        // The lowResultSet must be closed before commit.
        try (statement; var ps = lowPreparedStatement; commit; var f = resultSetFuture) {
            var rs = this.lowResultSet;
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("ResultSet close error", e);
        }
    }

    /**
     * Finish processing.
     *
     * @throws SQLException if a database access error occurs
     */
    protected void finish() throws SQLException {
        if (!finished) {
            this.finished = true;

            if (transaction.isAutoCommit()) {
                transaction.commit();
            }
        }
    }
}
