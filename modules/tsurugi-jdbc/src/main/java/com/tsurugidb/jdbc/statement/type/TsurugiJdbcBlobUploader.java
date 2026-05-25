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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi JDBC Blob uploader.
 *
 * @since 0.5.0
 */
public class TsurugiJdbcBlobUploader extends TsurugiJdbcLobUploader<InputStream> {

    /**
     * Creates a new instance.
     *
     * @param ownerPreparedStatement owner prepared statement
     */
    public TsurugiJdbcBlobUploader(TsurugiJdbcPreparedStatement ownerPreparedStatement) {
        super(ownerPreparedStatement);
    }

    @Override
    protected String getTmpFilePrefix() {
        return "blob";
    }

    @Override
    protected String getTmpFileSuffix() {
        return ".dat";
    }

    @Override
    protected void writeFile(InputStream value, Path dstFile) throws IOException {
        Files.copy(value, dstFile, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    protected FutureResponse<LargeObjectInfo> uploadValue(LargeObjectClient lowLargeObjectClient, InputStream value) throws IOException {
        return lowLargeObjectClient.upload(value);
    }
}
