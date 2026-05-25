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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC {@link TsurugiJdbcBlobReference} test.
 */
public class JdbcDbBlobReferenceTest extends JdbcDbTester {

    private static int SIZE = 5;

    private void setup(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("drop table if exists test");
            statement.executeUpdate("create table test(" //
                    + " pk int primary key," //
                    + " value blob" //
                    + ")" //
            );
        }
        try (var ps = connection.prepareStatement("insert into test values(?, ?)")) {
            connection.setAutoCommit(false);
            for (int i = 0; i < SIZE; i++) {
                ps.setInt(1, i);
                ps.setBlob(2, new ByteArrayInputStream(data(i)));
                ps.executeUpdate();
            }
            connection.commit();
        }
        connection.setAutoCommit(true);
    }

    private static byte[] data(int pk) {
        var bytes = new byte[pk];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, blob) -> {
                int length = (int) blob.length();
                assertEquals(expected.length, length);

                byte[] value = blob.getBytes(1, length);
                assertArrayEquals(expected, value);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getBytes(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, blob) -> {
                byte[] value = blob.getBytes(1, expected.length);
                assertArrayEquals(expected, value);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getBinaryStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, blob) -> {
                try (var is = blob.getBinaryStream()) {
                    byte[] value = is.readAllBytes();
                    assertArrayEquals(expected, value);
                }

                // twice
                try (var is = blob.getBinaryStream()) {
                    byte[] value = is.readAllBytes();
                    assertArrayEquals(expected, value);
                }

                // create cache
                blob.length();
                try (var is = blob.getBinaryStream()) {
                    byte[] value = is.readAllBytes();
                    assertArrayEquals(expected, value);
                }
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void getBinaryStream_pos_length(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection, (expected, blob) -> {
                if (expected.length < 2) {
                    try (var is = blob.getBinaryStream(1, expected.length)) {
                        byte[] value = is.readAllBytes();
                        assertArrayEquals(expected, value);
                    }
                } else {
                    try (var is = blob.getBinaryStream(2, expected.length)) {
                        byte[] value = is.readAllBytes();
                        assertArrayEquals(Arrays.copyOfRange(expected, 1, expected.length), value);
                    }
                }
            });
        }
    }

    @FunctionalInterface
    private interface BlobReferenceTester {
        void accept(byte[] expected, Blob blob) throws SQLException, IOException;
    }

    private void assertSelect(Connection connection, BlobReferenceTester tester) throws SQLException, IOException {
        setup(connection);
        try (var statement = connection.createStatement(); //
                var rs = statement.executeQuery("select * from test order by pk")) {
            while (rs.next()) {
                int pk = rs.getInt("pk");
                byte[] expected = data(pk);
                Blob blob = rs.getBlob("value");
                tester.accept(expected, blob);
            }
        }
    }
}
