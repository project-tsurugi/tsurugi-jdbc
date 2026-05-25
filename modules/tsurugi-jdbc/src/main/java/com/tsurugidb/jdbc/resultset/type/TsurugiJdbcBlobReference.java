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
import java.sql.Blob;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.statement.type.TsurugiJdbcBlob;
import com.tsurugidb.jdbc.util.TsurugiJdbcIoUtil;
import com.tsurugidb.tsubakuro.sql.BlobReference;

/**
 * Tsurugi JDBC Blob Reference.
 */
public class TsurugiJdbcBlobReference implements Blob {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final BlobReference lowBlob;
    private int timeout;
    private TsurugiJdbcBlob cachedBlob = null;

    /**
     * Creates a new instance.
     *
     * @param ownerResultSet result set
     * @param lowBlob        low-level blob reference
     */
    public TsurugiJdbcBlobReference(TsurugiJdbcResultSet ownerResultSet, BlobReference lowBlob) {
        this.ownerResultSet = ownerResultSet;
        this.lowBlob = lowBlob;
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
     * Open InputStream.
     *
     * @param timeout timeout
     * @param unit    time unit of timeout
     * @return InputStream
     * @throws SQLException if a database access error occurs
     */
    public InputStream openInputStream(long timeout, TimeUnit unit) throws SQLException {
        try {
            var transaction = ownerResultSet.getTransaction();
            var tx = transaction.getLowTransaction();
            var io = getIoUtil();
            return io.get(tx.openInputStream(lowBlob), timeout, unit);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("BLOB open error", e);
        }
    }

    private synchronized TsurugiJdbcBlob getCachedBlob() throws SQLException {
        if (this.cachedBlob == null) {
            byte[] data;
            try {
                data = openInputStream(timeout, TimeUnit.SECONDS).readAllBytes();
            } catch (IOException e) {
                throw getExceptionHandler().sqlException("BLOB read error", e);
            }
            this.cachedBlob = new TsurugiJdbcBlob(data);
        }
        return this.cachedBlob;
    }

    @Override
    public long length() throws SQLException {
        return getCachedBlob().length();
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        return getCachedBlob().getBytes(pos, length);
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        if (this.cachedBlob != null) {
            return cachedBlob.getBinaryStream();
        }

        return openInputStream(timeout, TimeUnit.SECONDS);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return getCachedBlob().getBinaryStream(pos, length);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return getCachedBlob().position(pattern, start);
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        return getCachedBlob().position(pattern, start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return getCachedBlob().setBytes(pos, bytes);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return getCachedBlob().setBytes(pos, bytes, offset, len);
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return getCachedBlob().setBinaryStream(pos);
    }

    @Override
    public void truncate(long len) throws SQLException {
        getCachedBlob().truncate(len);
    }

    @Override
    public void free() throws SQLException {
        if (this.cachedBlob != null) {
            cachedBlob.free();
        }
    }
}
