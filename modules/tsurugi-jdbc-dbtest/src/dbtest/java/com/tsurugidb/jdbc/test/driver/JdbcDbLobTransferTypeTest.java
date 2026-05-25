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
package com.tsurugidb.jdbc.test.driver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC BLOB test.
 */
public class JdbcDbLobTransferTypeTest extends JdbcDbTester {

    private static int SIZE = 5;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(JdbcDbLobTransferTypeTest.class);
        logInitStart(LOG, info);

        try (var connection = createConnection()) {
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
                    if (i == 0) {
                        ps.setNull(2, java.sql.Types.BLOB);
                    } else {
                        ps.setBlob(2, new ByteArrayInputStream(new byte[i - 1]));
                    }
                    ps.executeUpdate();
                }
                connection.commit();
            }
        }

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void jdbcUrl(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        String url = JdbcDbTestConnector.getJdbcUrlWithCredential();
        if (url.contains("?")) {
            url += "&";
        } else {
            url += "?";
        }
        url += "lobTransferType=" + lobTransferType;

        try (var connection = DriverManager.getConnection(url)) {
            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void tsurugiConfig(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var config = createTsurugiConfig();
        config.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void dataSource(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var dataSource = createDataSource();
        dataSource.setLobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = dataSource.getConnection()) {
            assertSelect(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void connectionBuilder(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        var builder = createConnectionBuilder();
        builder.lobTransferType(TsurugiJdbcLobTransferType.valueOf(lobTransferType));

        try (var connection = builder.build()) {
            assertSelect(connection);
        }
    }

    private void assertSelect(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); //
                var rs = statement.executeQuery("select * from test order by pk")) {
            while (rs.next()) {
                int pk = rs.getInt("pk");
                byte[] value = rs.getBytes("value");
                if (pk == 0) {
                    assertNull(value);
                } else {
                    assertArrayEquals(new byte[pk - 1], value);
                }
            }
        }
    }
}
