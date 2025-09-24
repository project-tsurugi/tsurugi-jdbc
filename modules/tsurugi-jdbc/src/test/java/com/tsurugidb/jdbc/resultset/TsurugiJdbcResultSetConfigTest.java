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
package com.tsurugidb.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionConfig;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatementConfig;

class TsurugiJdbcResultSetConfigTest {

    private final TsurugiJdbcFactory factory = new TsurugiJdbcFactory();

    @Test
    void getQueryTimeout() throws SQLException {
        var root = new TsurugiConfig();
        root.put(factory, "queryTimeout", "123");

        var connection = TsurugiJdbcConnectionConfig.of(root);
        var statement = TsurugiJdbcStatementConfig.of(connection);
        var target = TsurugiJdbcResultSetConfig.of(statement);

        assertEquals(123, target.getQueryTimeout());
        assertEquals(0, target.getDefaultTimeout());
    }

    @Test
    void getDefaultTimeout() throws SQLException {
        var root = new TsurugiConfig();
        root.put(factory, "defaultTimeout", "123");

        var connection = TsurugiJdbcConnectionConfig.of(root);
        var statement = TsurugiJdbcStatementConfig.of(connection);
        var target = TsurugiJdbcResultSetConfig.of(statement);

        assertEquals(123, target.getQueryTimeout());
        assertEquals(123, target.getDefaultTimeout());
    }
}
