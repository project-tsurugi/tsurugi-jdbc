/*
 * Copyright 2025-2026 Project Tsurugi.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.statement.type.TsurugiJdbcClob;
import com.tsurugidb.jdbc.util.TsurugiJdbcIoUtil;
import com.tsurugidb.jdbc.util.io.StringBuilderWriter;
import com.tsurugidb.tsubakuro.sql.ClobReference;

/**
 * Tsurugi JDBC Clob Reference.
 */
public class TsurugiJdbcClobReference implements Clob {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final ClobReference lowClob;
    private int timeout;
    private TsurugiJdbcClob cachedClob = null;

    /**
     * Creates a new instance.
     *
     * @param ownerResultSet result set
     * @param lowClob        low-level clob reference
     */
    public TsurugiJdbcClobReference(TsurugiJdbcResultSet ownerResultSet, ClobReference lowClob) {
        this.ownerResultSet = ownerResultSet;
        this.lowClob = lowClob;
        this.timeout = ownerResultSet.getConfig().getLobDownloadTimeout();
    }

    /**
     * Set download timeout.
     *
     * @param timeout download timeout [seconds]
     * @since 0.5.0
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
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
     * Get I/O utility.
     *
     * @return I/O utility
     */
    protected TsurugiJdbcIoUtil getIoUtil() {
        return ownerResultSet.getFactory().getIoUtil();
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
        var transaction = ownerResultSet.getTransaction();
        try {
            return transaction.executeOnly(tx -> {
                var io = getIoUtil();
                return io.get(tx.openReader(lowClob), timeout, unit);
            });
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("CLOB open error", e);
        }
    }

    private TsurugiJdbcClob getCachedClob() throws SQLException {
        if (this.cachedClob == null) {
            StringBuilder buffer;
            try (var reader = openReader(timeout, TimeUnit.SECONDS); //
                    var writer = new StringBuilderWriter(1024)) {
                reader.transferTo(writer);
                buffer = writer.getBuffer();
            } catch (IOException e) {
                throw getExceptionHandler().sqlException("CLOB read error", e);
            }
            this.cachedClob = new TsurugiJdbcClob(buffer);
        }
        return this.cachedClob;
    }

    @Override
    public long length() throws SQLException {
        return getCachedClob().length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return getCachedClob().getSubString(pos, length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        if (cachedClob != null) {
            return cachedClob.getCharacterStream();
        }

        return openReader(timeout, TimeUnit.SECONDS);
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return getCachedClob().getCharacterStream(pos, length);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return getCachedClob().getAsciiStream();
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        return getCachedClob().position(searchstr, start);
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return getCachedClob().position(searchstr, start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return getCachedClob().setString(pos, str);
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return getCachedClob().setString(pos, str, offset, len);
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return getCachedClob().setAsciiStream(pos);
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return getCachedClob().setCharacterStream(pos);
    }

    @Override
    public void truncate(long len) throws SQLException {
        getCachedClob().truncate(len);
    }

    @Override
    public void free() throws SQLException {
        if (cachedClob != null) {
            cachedClob.free();
        }
    }
}
