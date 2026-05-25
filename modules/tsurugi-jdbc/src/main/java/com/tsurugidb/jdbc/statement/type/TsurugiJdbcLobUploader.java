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
package com.tsurugidb.jdbc.statement.type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionConfig;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.tsubakuro.common.BlobTransferType;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi JDBC large object uploader.
 *
 * @param <T> value type
 * @since 0.5.0
 */
public abstract class TsurugiJdbcLobUploader<T> {

    private final TsurugiJdbcPreparedStatement ownerPreparedStatement;

    /**
     * Creates a new instance.
     *
     * @param ownerPreparedStatement owner prepared statement
     */
    protected TsurugiJdbcLobUploader(TsurugiJdbcPreparedStatement ownerPreparedStatement) {
        this.ownerPreparedStatement = ownerPreparedStatement;
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return ownerPreparedStatement.getFactory().getExceptionHandler();
    }

    /**
     * Upload value.
     *
     * @param value value to upload
     * @return uploaded large object info
     * @throws SQLException if an SQL error occurs while uploading the value
     */
    public LargeObjectInfo upload(T value) throws SQLException {
        try {
            var connection = ownerPreparedStatement.getConnection();
            var lowSession = connection.getLowSession();
            var lowLargeObjectClient = lowSession.getLargeObjectClient();
            var config = connection.getConfig();

            var lowTransferType = lowSession.getBlobTransferMedium().getBlobTransferType();
            if (lowTransferType == BlobTransferType.PRIVILEGED) {
                return uploadForPrivileged(lowLargeObjectClient, config, value);
            } else {
                return upload(lowLargeObjectClient, config, value);
            }
        } catch (Exception e) {
            throw getExceptionHandler().dataException("Upload large object error", e);
        }
    }

    private LargeObjectInfo uploadForPrivileged(LargeObjectClient lowLargeObjectClient, TsurugiJdbcConnectionConfig config, T value)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        Path tmpDir = config.getLobTmpDir();
        var tmpFile = Files.createTempFile(tmpDir, "tsurugiJDBC-" + getTmpFilePrefix(), getTmpFileSuffix());

        var parentDir = tmpFile.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        ownerPreparedStatement.addCloseable(() -> Files.deleteIfExists(tmpFile));
        writeFile(value, tmpFile);

        int timeout = getTimeout();
        return lowLargeObjectClient.upload(tmpFile).await(timeout, TimeUnit.SECONDS);
    }

    /**
     * Get temporary file prefix for privileged mode.
     *
     * @return temporary file prefix
     */
    protected abstract String getTmpFilePrefix();

    /**
     * Get temporary file suffix for privileged mode.
     *
     * @return temporary file suffix
     */
    protected abstract String getTmpFileSuffix();

    /**
     * Write value to file for privileged mode.
     *
     * @param value   value to write
     * @param dstFile temporary file path
     * @throws IOException if an I/O error occurs while writing the file
     */
    protected abstract void writeFile(T value, Path dstFile) throws IOException;

    private LargeObjectInfo upload(LargeObjectClient lowLargeObjectClient, TsurugiJdbcConnectionConfig config, T value) throws IOException, ServerException, InterruptedException, TimeoutException {
        int timeout = getTimeout();
        return uploadValue(lowLargeObjectClient, value).await(timeout, TimeUnit.SECONDS);
    }

    /**
     * Upload value (for non-privileged mode).
     *
     * @param lowLargeObjectClient LargeObjectClient
     * @param value                value to upload
     * @return FutureResponse of LargeObjectInfo
     * @throws IOException if an I/O error occurs while uploading the value
     */
    protected abstract FutureResponse<LargeObjectInfo> uploadValue(LargeObjectClient lowLargeObjectClient, T value) throws IOException;

    /**
     * Get upload timeout.
     *
     * @return upload timeout [seconds]
     */
    protected int getTimeout() {
        var config = ownerPreparedStatement.getConfig();
        return config.getLobUploadTimeout();
    }
}
