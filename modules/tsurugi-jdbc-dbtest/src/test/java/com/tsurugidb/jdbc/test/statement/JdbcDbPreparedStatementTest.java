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
import java.sql.SQLFeatureNotSupportedException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link TsurugiJdbcPreparedStatement} test.
 */
public class JdbcDbPreparedStatementTest extends JdbcDbTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws SQLException {
        try (var connection = createConnection()) {
            try (var ps = connection.prepareStatement("drop table if exists test")) {
                ps.executeUpdate();
            }
            try (var ps = connection.prepareStatement("create table test(" //
                    + " foo int primary key," //
                    + " bar bigint," //
                    + " zzz varchar(10)" //
                    + ")")) {
                ps.executeUpdate();
            }
        }
    }

    @Test
    void executeQuery_autoCommit() throws SQLException {
        beforeInsert();

        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test where foo=?")) {
            assertTrue(connection.getAutoCommit());

            ps.setInt(1, 1);
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(11, rs.getLong(2));
                assertEquals("abc", rs.getString(3));

                assertFalse(rs.next());
            }
            {
                var e = assertThrows(SQLException.class, () -> connection.commit());
                assertEquals("Transaction not found", e.getMessage());
            }

            ps.setInt(1, 2);
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(22, rs.getLong(2));
                assertEquals("def", rs.getString(3));

                assertFalse(rs.next());
            }
            {
                var e = assertThrows(SQLException.class, () -> connection.commit());
                assertEquals("Transaction not found", e.getMessage());
            }

            ps.setInt(1, 3);
            try (var rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }
            {
                var e = assertThrows(SQLException.class, () -> connection.commit());
                assertEquals("Transaction not found", e.getMessage());
            }
        }
    }

    private void beforeInsert() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            assertTrue(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            assertEquals(1, ps.executeUpdate());
        }
    }

    @Test
    void executeQuery_manualCommit() throws SQLException {
        beforeInsert();

        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test where foo=?")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            ps.setInt(1, 1);
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(11, rs.getLong(2));
                assertEquals("abc", rs.getString(3));

                assertFalse(rs.next());
            }

            connection.commit();
            {
                var e = assertThrows(SQLException.class, () -> connection.commit());
                assertEquals("Transaction not found", e.getMessage());
            }

            ps.setInt(1, 2);
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(22, rs.getLong(2));
                assertEquals("def", rs.getString(3));

                assertFalse(rs.next());
            }

            ps.setInt(1, 3);
            try (var rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }

            connection.commit();
        }
    }

    @Test
    void executeUpdate_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            assertTrue(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            assertEquals(1, ps.executeUpdate());

            executeQuery();
        }
    }

    private void executeQuery() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            assertTrue(connection.getAutoCommit());

            try (var rs = ps.executeQuery()) {
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
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            assertEquals(1, ps.executeUpdate());

            executeQuery0();

            connection.commit();

            executeQuery();
        }
    }

    private void executeQuery0() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }

            connection.commit();
        }
    }

//  @Test
//  void executeUpdate_closePrevSql() throws SQLException {

    @Test
    void executeQuery_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = ps.executeQuery()) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                try (var rs2 = ps.executeQuery()) {

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
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            connection.setAutoCommit(false);

            for (int i = 0; i < SIZE; i++) {
                ps.setInt(1, i);
                ps.setLong(2, i);
                ps.setString(3, Integer.toString(i));
                int count = ps.executeUpdate();
                assertEquals(1, count);
            }

            connection.commit();
        }
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            assertEquals(0, ps.getMaxRows());

            try (var rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(SIZE, count);
            }

            ps.setMaxRows(SIZE / 2);
            assertEquals(SIZE / 2, ps.getMaxRows());

            try (var rs = ps.executeQuery()) {
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
                var ps = connection.prepareStatement("select * from test")) {
            assertEquals(0, ps.getQueryTimeout());

            ps.setQueryTimeout(123);
            assertEquals(123, ps.getQueryTimeout());
        }
    }

    @Test
    void execute_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?), (?, ?, ?)")) {
            assertTrue(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            ps.setInt(4, 2);
            ps.setLong(5, 22);
            ps.setString(6, "def");

            boolean isSelect = ps.execute();
            assertFalse(isSelect);
            assertEquals(2, ps.getUpdateCount());
            assertNull(ps.getResultSet());

            execute_select();
        }
    }

    private void execute_select() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            assertTrue(connection.getAutoCommit());

            boolean isSelect = ps.execute();
            assertTrue(isSelect);
            assertEquals(-1, ps.getUpdateCount());
            try (var rs = ps.getResultSet()) {
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
                var ps = connection.prepareStatement("insert into test values(?, ?, ?), (?, ?, ?)")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            ps.setInt(4, 2);
            ps.setLong(5, 22);
            ps.setString(6, "def");

            boolean isSelect = ps.execute();
            assertFalse(isSelect);
            assertEquals(2, ps.getUpdateCount());
            assertNull(ps.getResultSet());

            execute_select0();

            connection.commit();

            execute_select();
        }
    }

    private void execute_select0() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = ps.execute();
            assertTrue(isSelect);
            assertEquals(-1, ps.getUpdateCount());
            try (var rs = ps.getResultSet()) {
                assertFalse(rs.next());
            }

            connection.commit();
        }
    }

    @Test
    void execute_closePrevSql() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            boolean isSelect = ps.execute();
            assertTrue(isSelect);
            try (var rs = ps.getResultSet()) {

                // 前のSQLが終わっていない状態で、同一statementを使って新しいSQLを実行
                isSelect = ps.execute();
                assertTrue(isSelect);
                try (var rs2 = ps.getResultSet()) {
                    assertFalse(rs2.isClosed());

                    // 前のResultSetは自動的にクローズされる
                    assertTrue(rs.isClosed());
                    var e = assertThrows(SQLException.class, () -> {
                        rs.next();
                    });
                    assertTrue(e.getMessage().contains("already closed"));

                    assertFalse(rs2.next());
                }
            }
        }
    }

    @Test
    void executeBatch_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            assertTrue(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            ps.addBatch();

            assertThrows(SQLFeatureNotSupportedException.class, () -> {
                ps.addBatch("insert into test values(2, 22, 'def')");
            });

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            ps.addBatch();

            int[] count = ps.executeBatch();
            assertArrayEquals(new int[] { 1, 1 }, count);

            executeQuery();
        }
    }

    @Test
    void executeBatch_manualCommit() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            ps.addBatch();

            int[] count = ps.executeBatch();
            assertArrayEquals(new int[] { 1, 1 }, count);

            executeQuery0();

            connection.commit();

            executeQuery();
        }
    }

//  @Test
//  void executeBatch_closePrevSql() throws SQLException {

    @Test
    void clearBatch() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test valeus(?, ?, ?)")) {
            assertArrayEquals(new int[0], ps.executeBatch());

            ps.setInt(1, 1);
            ps.setLong(2, 11);
            ps.setString(3, "abc");
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setLong(2, 22);
            ps.setString(3, "def");
            ps.addBatch();

            ps.clearBatch();
            assertArrayEquals(new int[0], ps.executeBatch());
        }

        executeQuery0();
    }

    @Test
    void closeOnCompletion() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            assertFalse(ps.isCloseOnCompletion());

            try (var rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }

            assertFalse(ps.isClosed());
        }

        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            ps.closeOnCompletion();
            assertTrue(ps.isCloseOnCompletion());

            try (var rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }

            assertTrue(ps.isClosed());
        }
    }

    @Test
    void close() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {

            try (var rs = ps.executeQuery()) {
                assertFalse(rs.isClosed());

                assertFalse(ps.isClosed());
                ps.close();
                assertTrue(ps.isClosed());

                assertTrue(rs.isClosed());
            }
        }

        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {

            boolean isSelect = ps.execute();
            assertTrue(isSelect);
            try (var rs = ps.getResultSet()) {
                assertFalse(rs.isClosed());

                assertFalse(ps.isClosed());
                ps.close();
                assertTrue(ps.isClosed());

                assertTrue(rs.isClosed());
            }
        }
    }
}
