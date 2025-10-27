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
package com.tsurugidb.jdbc.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.jdbc.test.util.JdbcDbTester;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;

/**
 * Tsurugi JDBC transaction test.
 */
public class JdbcDbTransactionTest extends JdbcDbTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws SQLException {
        try (var connection = createConnection()) {
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists test");
                statement.executeUpdate("create table test(" //
                        + " foo int primary key," //
                        + " bar bigint," //
                        + " zzz varchar(10)" //
                        + ")" //
                );
            }
            try (var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
                connection.setAutoCommit(false);
                for (int i = 0; i < 4; i++) {
                    ps.setInt(1, i);
                    ps.setLong(2, i);
                    ps.setString(3, Integer.toString(i));
                    ps.executeUpdate();
                }
                connection.commit();
            }
        }
    }

    @Test
    void LTX_OCC_manualCommit() throws SQLException {
        try (var connection1 = createConnection(); //
                var connection2 = createConnection(); //
                var statement1 = connection1.createStatement(); //
                var statement2 = connection2.createStatement()) {
            connection1.setTransactionType(TsurugiJdbcTransactionType.LTX);
            connection1.setWritePreserve(List.of("test"));
            connection1.setAutoCommit(false);

            connection2.setTransactionType(TsurugiJdbcTransactionType.OCC);
            connection2.setAutoCommit(true);

            int count = statement1.executeUpdate("insert into test values(9, 9, '9')");
            assertEquals(1, count);

            try (var rs = statement2.executeQuery("select * from test")) {
                var e = assertThrows(SQLTransactionRollbackException.class, () -> {
                    rs.next();
                });
                assertEquals("40001", e.getSQLState());
            }

            connection1.commit();
        }
    }

    @Test
    void LTX_OCC_autoCommit() throws SQLException {
        try (var connection1 = createConnection(); //
                var connection2 = createConnection(); //
                var statement1 = connection1.createStatement(); //
                var statement2 = connection2.createStatement()) {
            connection1.setTransactionType(TsurugiJdbcTransactionType.LTX);
            connection1.setWritePreserve(List.of("test"));
            connection1.setAutoCommit(true);

            connection2.setTransactionType(TsurugiJdbcTransactionType.OCC);
            connection2.setAutoCommit(true);

            int count = statement1.executeUpdate("insert into test values(9, 9, '9')");
            assertEquals(1, count);

            try (var rs = statement2.executeQuery("select * from test")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void LTX_RTX() throws SQLException {
        try (var connection1 = createConnection(); //
                var connection2 = createConnection(); //
                var statement1 = connection1.createStatement(); //
                var statement2 = connection2.createStatement()) {
            connection1.setTransactionType(TsurugiJdbcTransactionType.LTX);
            connection1.setWritePreserve(List.of("test"));
            connection1.setAutoCommit(false);

            connection2.setTransactionType(TsurugiJdbcTransactionType.RTX);
            connection2.setAutoCommit(true);

            int count = statement1.executeUpdate("insert into test values(9, 9, '9')");
            assertEquals(1, count);

            try (var rs = statement2.executeQuery("select * from test")) {
                assertTrue(rs.next());
            }

            connection1.commit();
        }
    }
}
