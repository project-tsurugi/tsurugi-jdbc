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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC DatabaseMetaData test.
 */
public class JdbcDbDatabaseMetaDataTest extends JdbcDbTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws SQLException {
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
        }
    }

    @Test
    void test() throws SQLException {
        try (var connection = createConnection()) {
            getUrl(connection);
            getUserName(connection);
            getDriverVersion(connection);
            getTableTypes(connection);
            getColumns(connection);
            getPrimaryKeys(connection);
            getTypeInfo(connection);
            getConnection(connection);
            getClientInfoProperties(connection);
            getPseudoColumns(connection);
        }
    }

    void getUrl(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        String url = metaData.getURL();

        String expected = JdbcDbTestConnector.getJdbcUrl();
        assertEquals(expected, url);
    }

    void getUserName(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        String user = metaData.getUserName();

        String expected = getUserNameFromIceaxe();
        assertEquals(expected, user);
    }

    void getDriverVersion(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        String version = metaData.getDriverVersion();
        int major = metaData.getDriverMajorVersion();
        int minor = metaData.getDriverMinorVersion();

        String[] ss = version.split(Pattern.quote("."));
        assertEquals(Integer.parseInt(ss[0]), major);
        assertEquals(Integer.parseInt(ss[1]), minor);
    }

    private String getUserNameFromIceaxe() {
        var connector = TsurugiConnector.of(JdbcDbTestConnector.getEndPoint(), JdbcDbTestConnector.getIceaxeCredential());
        try (var session = connector.createSession()) {
            return session.getUserName().orElse(null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void getTableTypes(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getTableTypes()) {
            assertTrue(rs.next());
            String tableType = rs.getString("TABLE_TYPE");
            assertEquals("TABLE", tableType);

            assertFalse(rs.next());
        }
    }

    void getColumns(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getColumns(null, null, "test", "%")) {
            assertTrue(rs.next());
            assertEquals("test", rs.getString("TABLE_NAME"));
            assertEquals("foo", rs.getString("COLUMN_NAME"));
            assertEquals(java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));
            assertEquals(1, rs.getInt("ORDINAL_POSITION"));
            assertEquals("NO", rs.getString("IS_NULLABLE"));

            assertTrue(rs.next());
            assertEquals("test", rs.getString("TABLE_NAME"));
            assertEquals("bar", rs.getString("COLUMN_NAME"));
            assertEquals(java.sql.Types.BIGINT, rs.getInt("DATA_TYPE"));
            assertEquals(2, rs.getInt("ORDINAL_POSITION"));
            assertEquals("YES", rs.getString("IS_NULLABLE"));

            assertTrue(rs.next());
            assertEquals("test", rs.getString("TABLE_NAME"));
            assertEquals("zzz", rs.getString("COLUMN_NAME"));
            assertEquals(java.sql.Types.VARCHAR, rs.getInt("DATA_TYPE"));
            assertEquals(10, rs.getInt("COLUMN_SIZE"));
            assertEquals(3, rs.getInt("ORDINAL_POSITION"));
            assertEquals("YES", rs.getString("IS_NULLABLE"));

            assertFalse(rs.next());
        }
    }

    void getPrimaryKeys(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getPrimaryKeys(null, null, "test")) {
            assertTrue(rs.next());
            assertEquals("test", rs.getString("TABLE_NAME"));
            assertEquals("foo", rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getInt("KEY_SEQ"));

            assertFalse(rs.next());
        }
    }

    void getTypeInfo(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getTypeInfo()) {
            while (rs.next()) {
                String typeName = rs.getString("TYPE_NAME");
                assertNotNull(typeName);
            }
        }
    }

    void getConnection(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        Connection actual = metaData.getConnection();
        assertSame(connection, actual);
    }

    void getClientInfoProperties(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getClientInfoProperties()) {
            while (rs.next()) {
                String name = rs.getString("NAME");
                String defaultValue = rs.getString("DEFAULT_VALUE");
                switch (name) {
                case TsurugiConfig.TRANSACTION_TYPE:
                    assertEquals("OCC", defaultValue);
                    break;
                case TsurugiConfig.AUTO_COMMIT:
                    assertEquals("true", defaultValue);
                    break;
                default:
                    break;
                }
                assertNotNull(rs.getString("DESCRIPTION"));
            }
        }
    }

    void getPseudoColumns(Connection connection) throws SQLException {
        var metaData = connection.getMetaData();
        try (var rs = metaData.getPseudoColumns(null, null, "test", "%")) {
            assertFalse(rs.next());
        }
    }
}
