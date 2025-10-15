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
package com.tsurugidb.jdbc.test.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC connection setAutoCommit test.
 */
public class JdbcDbSetAutoCommitTest extends JdbcDbTester {

    private static final int SIZE = 4;

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
                for (int i = 0; i < SIZE; i++) {
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
    void setAutoCommit_false_to_true() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            int count = statement.executeUpdate("insert into test values(9, 9, '9')");
            assertEquals(1, count);
            assertEquals(SIZE, selectCount());

            // 同じautoCommitモードをセットしても何も起きない
            connection.setAutoCommit(false);
            assertEquals(SIZE, selectCount());

            // autoCommitモードを変更すると、自動的にコミットされる
            connection.setAutoCommit(true);
            assertEquals(SIZE + 1, selectCount());
        }
    }

    private int selectCount() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("select count(*) from test")) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    @Test
    void setAutoCommit_true_to_false() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test")) {
                assertTrue(rs.next());

                // 同じautoCommitモードをセットしても何も起きない
                connection.setAutoCommit(true);

                while (rs.next()) {
                }
            }
        }
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            assertTrue(connection.getAutoCommit());

            try (var rs = statement.executeQuery("select * from test")) {
                assertTrue(rs.next());

                // autoCommitモードを変更すると、自動的にコミットされる
                connection.setAutoCommit(false);

                var e = assertThrows(SQLException.class, () -> {
                    while (rs.next()) {
                    }
                });
                assertTrue(e.getMessage().contains("transaction already closed"), () -> e.getMessage());
            }
        }
    }
}
