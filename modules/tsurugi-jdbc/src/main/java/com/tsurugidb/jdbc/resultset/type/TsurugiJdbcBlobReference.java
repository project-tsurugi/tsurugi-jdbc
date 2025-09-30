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
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.util.TsurugiJdbcIoUtil;
import com.tsurugidb.tsubakuro.sql.BlobReference;

/**
 * Tsurugi JDBC Blob Reference.
 */
public class TsurugiJdbcBlobReference implements Blob {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final BlobReference lowBlob;

    /**
     * Creates a new instance.
     *
     * @param ownerResultSet result set
     * @param lowBlob        low-level blob reference
     */
    public TsurugiJdbcBlobReference(TsurugiJdbcResultSet ownerResultSet, BlobReference lowBlob) {
        this.ownerResultSet = ownerResultSet;
        this.lowBlob = lowBlob;
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
     * Open InputStream.
     *
     * @param timeout timeout
     * @param unit    time unit of timeout
     * @return InputStream
     * @throws SQLException if a database access error occurs
     */
    public InputStream openInputStream(long timeout, TimeUnit unit) throws SQLException {
        var transaction = ownerResultSet.getTransaction();
        try {
            return transaction.executeOnly(tx -> {
                var io = getIoUtil();
                return io.get(tx.openInputStream(lowBlob), timeout, unit);
            });
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("BLOB open error", e);
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public long length() throws SQLException {
        throw new SQLFeatureNotSupportedException("length not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public byte[] getBytes(long pos, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes not supported");
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        int timeout = 0; // TODO timeout
        return openInputStream(timeout, TimeUnit.SECONDS);
    }

    @Override
    @TsurugiJdbcNotSupported
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("position not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("position not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
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
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
    }
}
