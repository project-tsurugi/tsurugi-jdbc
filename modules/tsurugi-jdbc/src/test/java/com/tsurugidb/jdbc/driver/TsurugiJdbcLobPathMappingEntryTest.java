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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class TsurugiJdbcLobPathMappingEntryTest {

    @Test
    void parsePathMapping() {
        if (isWindows()) {
            var entry = TsurugiJdbcLobPathMappingEntry.parse("D:/tmp/client:/mnt/client");
            assertEquals(entry.clientPath(), Path.of("D:/tmp/client"));
            assertEquals(entry.serverPath(), "/mnt/client");
        } else {
            var entry = TsurugiJdbcLobPathMappingEntry.parse("/tmp/client:/mnt/client");
            assertEquals(entry.clientPath(), Path.of("/tmp/client"));
            assertEquals(entry.serverPath(), "/mnt/client");
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }
}
