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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.BlobReference;

public class TsurugiJdbcBlobReference implements Blob {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final BlobReference lowBlob;

    public TsurugiJdbcBlobReference(TsurugiJdbcResultSet owner, BlobReference blob) {
        this.ownerResultSet = owner;
        this.lowBlob = blob;
    }

    public InputStream openInputStream(long timeout, TimeUnit unit) throws SQLException {
        var transaction = ownerResultSet.getTransaction().getLowTransaction();
        try {
            return transaction.openInputStream(lowBlob).await(timeout, unit);
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            var factory = ownerResultSet.getFactory();
            throw factory.getExceptionHandler().sqlException("BLOB open error", e);
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
