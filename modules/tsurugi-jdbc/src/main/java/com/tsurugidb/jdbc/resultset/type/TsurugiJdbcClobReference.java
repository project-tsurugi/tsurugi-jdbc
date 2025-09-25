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
package com.tsurugidb.jdbc.resultset.type;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.tsubakuro.sql.ClobReference;

/**
 * Tsurugi JDBC Clob Reference.
 */
public class TsurugiJdbcClobReference implements Clob {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final ClobReference lowClob;

    /**
     * Creates a new instance.
     *
     * @param ownerResultSet result set
     * @param lowClob        low-level clob reference
     */
    public TsurugiJdbcClobReference(TsurugiJdbcResultSet ownerResultSet, ClobReference lowClob) {
        this.ownerResultSet = ownerResultSet;
        this.lowClob = lowClob;
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return ownerResultSet.getFactory().getExceptionHandler();
    }

    /**
     * Open Reader.
     *
     * @param timeout timeout
     * @param unit    time unit of timeout
     * @return Reader
     * @throws SQLException if a database access error occurs
     */
    public Reader openReader(long timeout, TimeUnit unit) throws SQLException {
        var transaction = ownerResultSet.getTransaction().getLowTransaction();
        try {
            return transaction.openReader(lowClob).await(timeout, unit);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("CLOB open error", e);
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public long length() throws SQLException {
        throw new SQLFeatureNotSupportedException("length not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getSubString(long pos, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSubString not supported");
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        int timeout = 0; // TODO timeout
        return openReader(timeout, TimeUnit.SECONDS);
    }

    @Override
    @TsurugiJdbcNotSupported
    public InputStream getAsciiStream() throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public long position(String searchstr, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("position not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public long position(Clob searchstr, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("position not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int setString(long pos, String str) throws SQLException {
        throw new SQLFeatureNotSupportedException("setString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException("setString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException("truncate not supported");
    }

    @Override
    public void free() throws SQLException {
        // do nothing
    }

    @Override
    @TsurugiJdbcNotSupported
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
    }
}
