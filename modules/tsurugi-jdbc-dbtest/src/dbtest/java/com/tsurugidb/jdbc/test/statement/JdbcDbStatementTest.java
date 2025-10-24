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
package com.tsurugidb.jdbc.test.statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link TsurugiJdbcStatement} test.
 */
public class JdbcDbStatementTest extends JdbcDbTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            statement.executeUpdate("drop table if exists test");
            statement.executeUpdate("create table test(" //
                    + " foo int primary key," //
                    + " bar bigint," //
                    + " zzz varchar(10)" //
                    + ")" //
            );
        }
    }

    @Test
    void executeUpdate_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            int count = statement.executeUpdate("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
            assertEquals(2, count);

            executeQuery();
        }
    }

    private void executeQuery() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(11, rs.getLong(2));
                assertEquals("abc", rs.getString(3));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(22, rs.getLong(2));
                assertEquals("def", rs.getString(3));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void executeUpdate_manualCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            int count = statement.executeUpdate("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
            assertEquals(2, count);

            executeQuery0();

            connection.commit();

            executeQuery();
        }
    }

    private void executeQuery0() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.next());
            }

            connection.commit();
        }
    }

    @Test
    void executeUpdate_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                int count = statement.executeUpdate("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
                assertEquals(2, count);

                // 前のResultSetは自動的にクローズされる
                assertTrue(rs.isClosed());
                var e = assertThrows(SQLException.class, () -> {
                    rs.next();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }
    }

    @Test
    void executeQuery_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                try (var rs2 = statement.executeQuery("select * from test")) {

                    // 前のResultSetは自動的にクローズされる
                    assertTrue(rs.isClosed());

                    assertFalse(rs2.next());
                }
            }
        }
    }

    @Test
    void setMaxRows() throws SQLException {
        int SIZE = 10;

        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            for (int i = 0; i < SIZE; i++) {
                String sql = String.format("insert into test values(%d, %d, '%d')", i, i, i);
                int count = statement.executeUpdate(sql);
                assertEquals(1, count);
            }

            connection.commit();
        }
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertEquals(0, statement.getMaxRows());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(SIZE, count);
            }

            statement.setMaxRows(SIZE / 2);
            assertEquals(SIZE / 2, statement.getMaxRows());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(SIZE / 2, count);
            }
        }
    }

    @Test
    void setQueryTimeout() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertEquals(0, statement.getQueryTimeout());

            statement.setQueryTimeout(123);
            assertEquals(123, statement.getQueryTimeout());
        }
    }

    @Test
    void execute_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            boolean isSelect = statement.execute("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
            assertFalse(isSelect);
            assertEquals(2, statement.getUpdateCount());
            assertNull(statement.getResultSet());

            execute_select();
        }
    }

    private void execute_select() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            boolean isSelect = statement.execute("select * from test order by foo");
            assertTrue(isSelect);
            assertEquals(-1, statement.getUpdateCount());
            try (var rs = statement.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(11, rs.getLong(2));
                assertEquals("abc", rs.getString(3));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(22, rs.getLong(2));
                assertEquals("def", rs.getString(3));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void execute_manualCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = statement.execute("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
            assertFalse(isSelect);
            assertEquals(2, statement.getUpdateCount());
            assertNull(statement.getResultSet());

            execute_select0();

            connection.commit();

            execute_select();
        }
    }

    private void execute_select0() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = statement.execute("select * from test order by foo");
            assertTrue(isSelect);
            assertEquals(-1, statement.getUpdateCount());
            try (var rs = statement.getResultSet()) {
                assertFalse(rs.next());
            }

            connection.commit();
        }
    }

    @Test
    void execute_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = statement.execute("select * from test order by foo");
            assertTrue(isSelect);
            try (var rs = statement.getResultSet()) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                isSelect = statement.execute("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
                assertFalse(isSelect);
                assertEquals(2, statement.getUpdateCount());

                // 前のResultSetは自動的にクローズされる
                assertTrue(rs.isClosed());
                var e = assertThrows(SQLException.class, () -> {
                    rs.next();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }
    }

    @Test
    void executeBatch_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            statement.addBatch("insert into test values(1, 11, 'abc')");
            statement.addBatch("insert into test values(2, 22, 'def')");
            int[] count = statement.executeBatch();
            assertArrayEquals(new int[] { 1, 1 }, count);

            executeQuery();
        }
    }

    @Test
    void executeBatch_manualCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            statement.addBatch("insert into test values(1, 11, 'abc')");
            statement.addBatch("insert into test values(2, 22, 'def')");
            int[] count = statement.executeBatch();
            assertArrayEquals(new int[] { 1, 1 }, count);

            executeQuery0();

            connection.commit();

            executeQuery();
        }
    }

    @Test
    void executeBatch_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = statement.execute("select * from test order by foo");
            assertTrue(isSelect);
            try (var rs = statement.getResultSet()) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                statement.addBatch("insert into test values(1, 11, 'abc'), (2, 22, 'def')");
                statement.addBatch("insert into test values(3, 33, 'ghi')");
                int[] count = statement.executeBatch();
                assertArrayEquals(new int[] { 2, 1 }, count);

                // 前のResultSetは自動的にクローズされる
                assertTrue(rs.isClosed());
                var e = assertThrows(SQLException.class, () -> {
                    rs.next();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 1, 2, 10 })
    void executeBatch_queueSize(int queueSize) throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());
            statement.setBatchQueueSize(queueSize);

            statement.addBatch("insert into test values(1, 1, '')");
            statement.addBatch("insert into test values(21, 2, ''), (22, 2, '')");
            statement.addBatch("insert into test values(31, 3, ''), (32, 3, ''),  (33, 3, '')");
            statement.addBatch("insert into test values(4, 4, '')");
            int[] count = statement.executeBatch();
            assertArrayEquals(new int[] { 1, 2, 3, 1 }, count);
        }
    }

    @Test
    void clearBatch() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertArrayEquals(new int[0], statement.executeBatch());

            statement.addBatch("insert into test values(1, 11, 'abc')");
            statement.addBatch("insert into test values(2, 22, 'def')");
            statement.clearBatch();
            assertArrayEquals(new int[0], statement.executeBatch());
        }

        executeQuery0();
    }

    @Test
    void closeOnCompletion() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertFalse(statement.isCloseOnCompletion());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.next());
            }

            assertFalse(statement.isClosed());
        }

        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            statement.closeOnCompletion();
            assertTrue(statement.isCloseOnCompletion());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.next());
            }

            assertTrue(statement.isClosed());
        }
    }

    @Test
    void close() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.isClosed());

                assertFalse(statement.isClosed());
                statement.close();
                assertTrue(statement.isClosed());

                assertTrue(rs.isClosed());
            }
        }

        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {

            boolean isSelect = statement.execute("select * from test order by foo");
            assertTrue(isSelect);
            try (var rs = statement.getResultSet()) {
                assertFalse(rs.isClosed());

                assertFalse(statement.isClosed());
                statement.close();
                assertTrue(statement.isClosed());

                assertTrue(rs.isClosed());
            }
        }
    }
}
