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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC DatabaseMetaData getTables test.
 */
public class JdbcDbGetTablesTest extends JdbcDbTester {

    @Test
    void getTables() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            statement.executeUpdate("drop table if exists test");
            {
                var metaData = connection.getMetaData();
                boolean found = false;
                try (var rs = metaData.getTables(null, null, "%", null)) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        if (tableName.equals("test")) {
                            found = true;
                        }
                        String tableType = rs.getString("TABLE_TYPE");
                        assertEquals("TABLE", tableType);
                    }
                    assertFalse(found);
                }
            }

            statement.executeUpdate("create table test(" //
                    + " foo int primary key," //
                    + " bar bigint," //
                    + " zzz varchar(10)" //
                    + ")" //
            );
            {
                var metaData = connection.getMetaData();
                boolean found = false;
                try (var rs = metaData.getTables(null, null, "%", null)) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        if (tableName.equals("test")) {
                            found = true;
                        }
                        String tableType = rs.getString("TABLE_TYPE");
                        assertEquals("TABLE", tableType);
                    }
                    assertTrue(found);
                }
            }
        }
    }
}
