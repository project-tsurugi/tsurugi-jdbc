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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link TsurugiJdbcPreparedStatement} setNull test.
 */
public class JdbcDbSetNullTest extends JdbcDbTester {

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
    void setNull() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, java.sql.Types.BIGINT);
            ps.setNull(3, java.sql.Types.VARCHAR);
            assertEquals(1, ps.executeUpdate());
        }

        assertSelect();
    }

    @Test
    void setNull_NULL() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, java.sql.Types.NULL);
            ps.setNull(3, java.sql.Types.NULL);
            assertEquals(1, ps.executeUpdate());
        }

        assertSelect();
    }

    @Test
    void setNull_OTHER() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, java.sql.Types.OTHER);
            ps.setNull(3, java.sql.Types.OTHER);
            assertEquals(1, ps.executeUpdate());
        }

        assertSelect();
    }

    private void assertSelect() throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("select * from test order by foo")) {
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.wasNull());
                assertEquals(0, rs.getLong(2));
                assertTrue(rs.wasNull());
                assertNull(rs.getString(3));
                assertTrue(rs.wasNull());

                assertFalse(rs.next());
            }
        }
    }
}
