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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
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
 * Tsurugi JDBC BLOB test.
 */
public class JdbcDbTypeBlob2Test extends JdbcDbTester {

    private static int SIZE = 5;

    @BeforeEach
    void beforeEach(TestInfo info) throws SQLException {
        try (var connection = createConnection()) {
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists test");
                statement.executeUpdate("create table test(" //
                        + " pk int primary key," //
                        + " value blob" //
                        + ")" //
                );
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setBlob_Blob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setBlob(2, (Blob) null);
                    } else {
                        var blob = connection.createBlob();
                        blob.setBytes(1, data(i));
                        ps.setBlob(2, blob);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setBlob_InputStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setBlob(2, (InputStream) null);
                    } else {
                        ps.setBlob(2, new ByteArrayInputStream(data(i)));
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setBlob_InputStream_length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setBlob(2, (InputStream) null, 0);
                    } else {
                        var data = data(i);
                        var buf = new byte[data.length + 4];
                        System.arraycopy(data, 0, buf, 0, data.length);
                        long length = i - 1;
                        ps.setBlob(2, new ByteArrayInputStream(buf), length);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_Blob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setNull(2, java.sql.Types.BLOB);
                    } else {
                        var blob = connection.createBlob();
                        blob.setBytes(1, data(i));
                        ps.setObject(2, blob);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_Blob_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (Blob) null, java.sql.Types.BLOB);
                    } else {
                        var blob = connection.createBlob();
                        blob.setBytes(1, data(i));
                        ps.setObject(2, blob, java.sql.Types.BLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_bytes_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (byte[]) null, java.sql.Types.BLOB);
                    } else {
                        ps.setObject(2, data(i), java.sql.Types.BLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void setObject_InputStream_sqlType(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    ps.setInt(1, i);
                    if (i == 0) {
                        ps.setObject(2, (InputStream) null, java.sql.Types.BLOB);
                    } else {
                        ps.setObject(2, new ByteArrayInputStream(data(i)), java.sql.Types.BLOB);
                    }
                    ps.executeUpdate();
                }
            }

            assertSelect(connection);
        }
    }

    private static byte[] data(int pk) {
        if (pk == 0) {
            return null;
        } else {
            var bytes = new byte[pk - 1];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) i;
            }
            return bytes;
        }
    }

    private void assertSelect(Connection connection) throws SQLException, IOException {
        testSelect(connection, this::assert_getBlob);
        testSelect(connection, this::assert_getBytes);
        testSelect(connection, this::assert_getObject);
    }

    @FunctionalInterface
    private interface BlobTester {
        void accept(byte[] expected, ResultSet rs) throws SQLException, IOException;
    }

    private void testSelect(Connection connection, BlobTester tester) throws SQLException, IOException {
        try (var statement = connection.createStatement(); //
                var rs = statement.executeQuery("select * from test order by pk")) {
            while (rs.next()) {
                int pk = rs.getInt("pk");
                byte[] expected = data(pk);
                tester.accept(expected, rs);
            }
        }
    }

    private void assert_getBlob(byte[] expected, ResultSet rs) throws SQLException, IOException {
        Blob blob = rs.getBlob(2);
        assertBlob(expected, blob);
    }

    private void assertBlob(byte[] expected, Blob blob) throws SQLException, IOException {
        if (expected == null) {
            assertNull(blob);
        } else {
            try (var is = blob.getBinaryStream(); var os = new ByteArrayOutputStream()) {
                is.transferTo(os);
                byte[] actual = os.toByteArray();
                assertArrayEquals(expected, actual);
            }
        }
    }

    private void assert_getBytes(byte[] expected, ResultSet rs) throws SQLException, IOException {
        byte[] value = rs.getBytes(2);
        assertArrayEquals(expected, value);
    }

    private void assert_getObject(byte[] expected, ResultSet rs) throws SQLException, IOException {
        Blob blob = (Blob) rs.getObject(2);
        assertBlob(expected, blob);
    }
}
