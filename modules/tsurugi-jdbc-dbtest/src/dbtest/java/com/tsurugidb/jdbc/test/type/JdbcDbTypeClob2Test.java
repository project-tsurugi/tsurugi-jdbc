/*
 * Copyright 2025-2026 Project Tsurugi.
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
package com.tsurugidb.jdbc.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC CLOB test.
 */
public class JdbcDbTypeClob2Test extends JdbcDbTester {

    private static int SIZE = 5;

    @BeforeEach
    void beforeEach(TestInfo info) throws SQLException {
        try (var connection = createConnection()) {
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists test");
                statement.executeUpdate("create table test(" //
                        + " pk int primary key," //
                        + " value clob" //
                        + ")" //
                );
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setClob_Clob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setClob(2, (Clob) null);
                    } else {
                        var clob = connection.createClob();
                        clob.setString(1, data(i));
                        ps.setClob(2, clob);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setClob_Reader(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setClob(2, (Reader) null);
                    } else {
                        ps.setClob(2, new StringReader(data(i)));
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setClob_String_length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setClob(2, (Reader) null, 0);
                    } else {
                        var data = data(i);
                        String buf = data + "XXXX";
                        long length = i - 1;
                        ps.setClob(2, new StringReader(buf), length);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_Clob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setNull(2, java.sql.Types.CLOB);
                    } else {
                        var clob = connection.createClob();
                        clob.setString(1, data(i));
                        ps.setObject(2, clob);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_Clob_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (Clob) null, java.sql.Types.CLOB);
                    } else {
                        var clob = connection.createClob();
                        clob.setString(1, data(i));
                        ps.setObject(2, clob, java.sql.Types.CLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_String_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (String) null, java.sql.Types.CLOB);
                    } else {
                        ps.setObject(2, data(i), java.sql.Types.CLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_Reader_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (Reader) null, java.sql.Types.CLOB);
                    } else {
                        ps.setObject(2, new StringReader(data(i)), java.sql.Types.CLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    private static String data(int pk) {
        if (pk == 0) {
            return null;
        } else {
            var chars = new char[pk - 1];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = (char) (('A' + i) % ('Z' - 'A' + 1));
            }
            return new String(chars);
        }
    }

    private void assertSelect(Connection connection) throws SQLException, IOException {
        testSelect(connection, this::assert_getClob);
        testSelect(connection, this::assert_getString);
        testSelect(connection, this::assert_getObject);
    }

    @FunctionalInterface
    private interface ClobTester {
        void accept(String expected, ResultSet rs) throws SQLException, IOException;
    }

    private void testSelect(Connection connection, ClobTester tester) throws SQLException, IOException {
        try (var statement = connection.createStatement(); //
                var rs = statement.executeQuery("select * from test order by pk")) {
            while (rs.next()) {
                int pk = rs.getInt("pk");
                String expected = data(pk);
                tester.accept(expected, rs);
            }
        }
    }

    private void assert_getClob(String expected, ResultSet rs) throws SQLException, IOException {
        Clob clob = rs.getClob(2);
        assertClob(expected, clob);
    }

    private void assertClob(String expected, Clob clob) throws SQLException, IOException {
        if (expected == null) {
            assertNull(clob);
        } else {
            try (var reader = clob.getCharacterStream(); var writer = new StringWriter()) {
                reader.transferTo(writer);
                String actual = writer.toString();
                assertEquals(expected, actual);
            }
        }
    }

    private void assert_getString(String expected, ResultSet rs) throws SQLException, IOException {
        String value = rs.getString(2);
        assertEquals(expected, value);
    }

    private void assert_getObject(String expected, ResultSet rs) throws SQLException, IOException {
        Clob clob = (Clob) rs.getObject(2);
        assertClob(expected, clob);
    }
}
