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
package com.tsurugidb.jdbc.test.resultset.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC {@link TsurugiJdbcClobReference} test.
 */
public class JdbcDbClobReferenceTest extends JdbcDbTester {

    private static int SIZE = 5;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(JdbcDbClobReferenceTest.class);
        logInitStart(LOG, info);

        try (var connection = createConnection()) {
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists test");
                statement.executeUpdate("create table test(" //
                        + " pk int primary key," //
                        + " value clob" //
                        + ")" //
                );
            }
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                connection.setAutoCommit(false);
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    ps.setClob(2, new StringReader(data(i)));
                    ps.executeUpdate();
                }
                connection.commit();
            }
        }

        logInitEnd(LOG, info);
    }

    private static String data(int pk) {
        var chars = new char[pk];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) (('A' + i) % ('Z' - 'A' + 1));
        }
        return new String(chars);
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, clob) -> {
                int length = (int) clob.length();
                assertEquals(expected.length(), length);

                String value = clob.getSubString(1, length);
                assertEquals(expected, value);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getSubString(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, clob) -> {
                String value = clob.getSubString(1, expected.length());
                assertEquals(expected, value);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getCharacterStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, clob) -> {
                try (var reader = clob.getCharacterStream(); var writer = new StringWriter()) {
                    reader.transferTo(writer);
                    String value = writer.toString();
                    assertEquals(expected, value);
                }

                // twice
                try (var reader = clob.getCharacterStream(); var writer = new StringWriter()) {
                    reader.transferTo(writer);
                    String value = writer.toString();
                    assertEquals(expected, value);
                }

                // create cache
                clob.length();
                try (var reader = clob.getCharacterStream(); var writer = new StringWriter()) {
                    reader.transferTo(writer);
                    String value = writer.toString();
                    assertEquals(expected, value);
                }
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getCharacterStream_pos_length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, clob) -> {
                if (expected.length() < 2) {
                    try (var reader = clob.getCharacterStream(1, expected.length()); var writer = new StringWriter()) {
                        reader.transferTo(writer);
                        String value = writer.toString();
                        assertEquals(expected, value);
                    }
                } else {
                    try (var reader = clob.getCharacterStream(2, expected.length()); var writer = new StringWriter()) {
                        reader.transferTo(writer);
                        String value = writer.toString();
                        assertEquals(expected.substring(1, expected.length()), value);
                    }
                }
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getAsciiStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, clob) -> {
                try (var is = clob.getAsciiStream(); var os = new ByteArrayOutputStream()) {
                    is.transferTo(os);
                    String value = new String(os.toByteArray());
                    assertEquals(expected, value);
                }
            });
        }
    }

    @FunctionalInterface
    private interface ClobReferenceTester {
        void accept(String expected, Clob clob) throws SQLException, IOException;
    }

    private void assertSelect(Connection connection, ClobReferenceTester tester) throws SQLException, IOException {
        try (var statement = connection.createStatement(); //
                var rs = statement.executeQuery("select * from test order by pk")) {
            while (rs.next()) {
                int pk = rs.getInt("pk");
                String expected = data(pk);
                Clob clob = rs.getClob("value");
                tester.accept(expected, clob);
            }
        }
    }
}
