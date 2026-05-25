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
package com.tsurugidb.jdbc.driver;

import java.nio.file.Path;

/**
 * Tsurugi JDBC large object path mapping entry.
 *
 * @since 0.5.0
 */
public final /* record */ class TsurugiJdbcLobPathMappingEntry {

    /**
     * Parses a path mapping string and creates a new instance.
     *
     * @param pathMapping path mapping string
     * @return a new instance
     */
    public static TsurugiJdbcLobPathMappingEntry parse(String pathMapping) {
        int n = pathMapping.lastIndexOf(':');
        if (n < 0) {
            throw new IllegalArgumentException("Invalid path mapping: " + pathMapping);
        }
        String clientPath = pathMapping.substring(0, n).trim();
        String serverPath = pathMapping.substring(n + 1).trim();
        return new TsurugiJdbcLobPathMappingEntry(Path.of(clientPath), serverPath);
    }

    private final Path clientPath;
    private final String serverPath;

    /**
     * Creates a new instance.
     *
     * @param clientPath client path
     * @param serverPath server path
     */
    public TsurugiJdbcLobPathMappingEntry(Path clientPath, String serverPath) {
        this.clientPath = clientPath;
        this.serverPath = serverPath;
    }

    /**
     * Get client path.
     *
     * @return client path
     */
    public Path clientPath() {
        return this.clientPath;
    }

    /**
     * Get server path.
     *
     * @return server path
     */
    public String serverPath() {
        return this.serverPath;
    }
}
