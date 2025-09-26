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
package com.tsurugidb.jdbc.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.exception.SqlState;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

class TsurugiJdbcUrlParserTest {

    private final TsurugiJdbcFactory factory = TsurugiJdbcFactory.getDefaultFactory();

    @Test
    void acceptsURL() throws SQLException {
        {
            boolean actual = TsurugiJdbcUrlParser.acceptUrl(factory, "jdbc:tsurugi:hoge");
            assertTrue(actual);
        }
        {
            boolean actual = TsurugiJdbcUrlParser.acceptUrl(factory, "jdbc:tsurugi1:hoge");
            assertFalse(actual);
        }

        var e = assertThrows(SQLNonTransientConnectionException.class, () -> {
            TsurugiJdbcUrlParser.acceptUrl(factory, null);
        });
        assertEquals(SqlState.S08001_UNABLE_TO_CONNECTION.code(), e.getSQLState());
    }

    @Test
    void parseWithProperties() throws Exception {
        String url = "jdbc:tsurugi:ipc:tsurugi?user=User&password=Password";
        var info = new Properties();
        info.setProperty("password", "zzz");
        TsurugiConfig actual = TsurugiJdbcUrlParser.parse(factory, url, info);

        assertEquals("ipc:tsurugi", actual.getEndpoint());
        assertEquals("User", actual.getUser());
        assertEquals("zzz", actual.getPassword());
    }

    @Test
    void parse() throws Exception {
        String url = "jdbc:tsurugi:ipc:tsurugi?user=User&password=Password";
        TsurugiConfig actual = TsurugiJdbcUrlParser.parse(factory, url);

        assertEquals("ipc:tsurugi", actual.getEndpoint());
        assertEquals("User", actual.getUser());
        assertEquals("Password", actual.getPassword());
    }

    @Test
    void createJdbcUrl() {
        String actual = TsurugiJdbcUrlParser.createJdbcUrl("hoge");
        assertEquals("jdbc:tsurugi:hoge", actual);
    }
}
