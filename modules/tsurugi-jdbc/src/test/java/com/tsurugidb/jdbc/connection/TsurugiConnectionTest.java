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
package com.tsurugidb.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

class TsurugiConnectionTest {

    private static TsurugiJdbcFactory factory = new TsurugiJdbcFactory();

    private static TsurugiJdbcConnection createTestConnection() {
        var session = new LowSessionTestMock();
        var properties = new TsurugiJdbcConnectionProperties();
        return factory.createConnection(session, properties);
    }

    @Test
    void unwrap() throws SQLException {
        try (Connection connection = createTestConnection()) {
            TsurugiJdbcConnection actual = connection.unwrap(TsurugiJdbcConnection.class);
            assertSame(connection, actual);
        }
    }

    @Test
    void isWrapperFor() throws SQLException {
        try (Connection connection = createTestConnection()) {
            boolean actual = connection.isWrapperFor(TsurugiJdbcConnection.class);
            assertTrue(actual);
        }
    }
}
