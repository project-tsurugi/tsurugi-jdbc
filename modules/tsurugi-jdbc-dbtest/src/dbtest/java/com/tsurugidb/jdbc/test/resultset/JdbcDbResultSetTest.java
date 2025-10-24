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
package com.tsurugidb.jdbc.test.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link TsurugiJdbcResultSet} test.
 */
public class JdbcDbResultSetTest extends JdbcDbTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(JdbcDbResultSetTest.class);
        logInitStart(LOG, info);

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
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    ps.setLong(2, i);
                    ps.setString(3, Integer.toString(i));
                    ps.executeUpdate();
                }
                connection.commit();
            }
        }

        logInitEnd(LOG, info);
    }

    @Test
    void findColumn() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertEquals(1, rs.findColumn("foo"));
                assertEquals(2, rs.findColumn("bar"));
                assertEquals(3, rs.findColumn("zzz"));

                var e = assertThrows(SQLSyntaxErrorException.class, () -> {
                    rs.findColumn("not found");
                });
                assertEquals("42703", e.getSQLState());
            }
        }
    }

    @Test
    void getRow() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertEquals(0, rs.getRow());
                assertTrue(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isAfterLast());

                int count = 0;
                assertTrue(rs.next());
                assertEquals(++count, rs.getRow());
                assertFalse(rs.isBeforeFirst());
                assertTrue(rs.isFirst());
                assertFalse(rs.isAfterLast());

                while (rs.next()) {
                    assertEquals(++count, rs.getRow());
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isAfterLast());
                }

                assertEquals(SIZE, rs.getRow());
                assertFalse(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertTrue(rs.isAfterLast());
            }
        }
    }

    @Test
    void isClosed_autoCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.isClosed());

                while (rs.next()) {
                    assertFalse(rs.isClosed());
                }

                assertTrue(rs.isClosed());
            }
        }
    }

    @Test
    void isClosed_manualCommit() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test order by foo")) {
                assertFalse(rs.isClosed());

                while (rs.next()) {
                    assertFalse(rs.isClosed());
                }

                assertFalse(rs.isClosed());

                rs.close();
                assertTrue(rs.isClosed());
            }
        }
    }
}
