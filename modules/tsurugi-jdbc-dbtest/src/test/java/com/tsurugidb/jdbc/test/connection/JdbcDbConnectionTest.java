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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

/**
 * {@link TsurugiJdbcConnection} test.
 */
public class JdbcDbConnectionTest extends JdbcDbTester {

    @Test
    void setTransactionType() throws SQLException {
        try (var connection = createConnection()) {
            assertEquals(TsurugiJdbcTransactionType.OCC, connection.getTransactionType());
            assertEquals(TransactionType.SHORT, connection.getConfig().getLowTransactionOption().getType());

            connection.setTransactionType(TsurugiJdbcTransactionType.LTX);
            assertEquals(TsurugiJdbcTransactionType.LTX, connection.getTransactionType());
            assertEquals(TransactionType.LONG, connection.getConfig().getLowTransactionOption().getType());
        }
    }

    @Test
    void setTransactionLabel() throws SQLException {
        try (var connection = createConnection()) {
            assertNull(connection.getTransactionLabel());
            assertEquals("", connection.getConfig().getLowTransactionOption().getLabel());

            connection.setTransactionLabel("test");
            assertEquals("test", connection.getTransactionLabel());
            assertEquals("test", connection.getConfig().getLowTransactionOption().getLabel());
        }
    }

    @Test
    void setTransactionIncludeDdl() throws SQLException {
        try (var connection = createConnection()) {
            assertFalse(connection.getTransactionIncludeDdl());
            assertFalse(connection.getConfig().getLowTransactionOption().getModifiesDefinitions());

            connection.setTransactionIncludeDdl(true);
            assertTrue(connection.getTransactionIncludeDdl());
            assertTrue(connection.getConfig().getLowTransactionOption().getModifiesDefinitions());
        }
    }

    @Test
    void setWritePreserve() throws SQLException {
        try (var connection = createConnection()) {
            assertNull(connection.getWritePreserve());
            assertEquals(List.of(), connection.getConfig().getLowTransactionOption().getWritePreservesList());

            connection.setWritePreserve(List.of("test"));
            assertEquals(List.of("test"), connection.getWritePreserve());
            assertEquals(List.of(WritePreserve.newBuilder().setTableName("test").build()), connection.getConfig().getLowTransactionOption().getWritePreservesList());
        }
    }

    @Test
    void setInclusiveReadArea() throws SQLException {
        try (var connection = createConnection()) {
            assertNull(connection.getInclusiveReadArea());
            assertEquals(List.of(), connection.getConfig().getLowTransactionOption().getInclusiveReadAreasList());

            connection.setInclusiveReadArea(List.of("test"));
            assertEquals(List.of("test"), connection.getInclusiveReadArea());
            assertEquals(List.of(ReadArea.newBuilder().setTableName("test").build()), connection.getConfig().getLowTransactionOption().getInclusiveReadAreasList());
        }
    }

    @Test
    void setExclusiveReadArea() throws SQLException {
        try (var connection = createConnection()) {
            assertNull(connection.getExclusiveReadArea());
            assertEquals(List.of(), connection.getConfig().getLowTransactionOption().getExclusiveReadAreasList());

            connection.setExclusiveReadArea(List.of("test"));
            assertEquals(List.of("test"), connection.getExclusiveReadArea());
            assertEquals(List.of(ReadArea.newBuilder().setTableName("test").build()), connection.getConfig().getLowTransactionOption().getExclusiveReadAreasList());
        }
    }

    @Test
    void setTransactionScanParallel() throws SQLException {
        try (var connection = createConnection()) {
            assertEquals(OptionalInt.empty(), connection.getTransactionScanParallel());
            assertEquals(0, connection.getConfig().getLowTransactionOption().getScanParallel());

            connection.setTransactionScanParallel(123);
            assertEquals(OptionalInt.of(123), connection.getTransactionScanParallel());
            assertEquals(123, connection.getConfig().getLowTransactionOption().getScanParallel());
        }
    }

    @Test
    void setAutoCommit() throws SQLException {
        try (var connection = createConnection()) {
            assertTrue(connection.getAutoCommit());

            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());
        }
    }

    @Test
    void setCommitType() throws SQLException {
        try (var connection = createConnection()) {
            assertEquals(TsurugiJdbcCommitType.DEFAULT, connection.getCommitType());
            assertEquals(CommitStatus.COMMIT_STATUS_UNSPECIFIED, connection.getConfig().getLowCommitOption().getNotificationType());

            connection.setCommitType(TsurugiJdbcCommitType.STORED);
            assertEquals(TsurugiJdbcCommitType.STORED, connection.getCommitType());
            assertEquals(CommitStatus.STORED, connection.getConfig().getLowCommitOption().getNotificationType());
        }
    }

    @Test
    void setCommitAutoDispose() throws SQLException {
        try (var connection = createConnection()) {
            assertFalse(connection.getCommitAutoDispose());
            assertFalse(connection.getConfig().getLowCommitOption().getAutoDispose());

            connection.setCommitAutoDispose(true);
            assertTrue(connection.getCommitAutoDispose());
            assertTrue(connection.getConfig().getLowCommitOption().getAutoDispose());
        }
    }

    @Test
    void commit_noTransaction() throws SQLException {
        try (var connection = createConnection()) {
            assertTrue(connection.getAutoCommit());
            {
                var e = assertThrows(SQLException.class, () -> connection.commit());
                assertEquals("Cannot commit when auto-commit is enabled", e.getMessage());
                assertEquals("25000", e.getSQLState());
            }
        }
        try (var connection = createConnection()) {
            connection.setAutoCommit(false);
            assertDoesNotThrow(() -> connection.commit());
        }
    }

    @Test
    void rollback_noTransaction() throws SQLException {
        try (var connection = createConnection()) {
            assertTrue(connection.getAutoCommit());
            {
                var e = assertThrows(SQLException.class, () -> connection.rollback());
                assertEquals("Cannot rollback when auto-commit is enabled", e.getMessage());
                assertEquals("25000", e.getSQLState());
            }
        }
        try (var connection = createConnection()) {
            connection.setAutoCommit(false);
            assertDoesNotThrow(() -> connection.rollback());
        }
    }

    @Test
    void setReadOnly() throws SQLException {
        try (var connection = createConnection()) {
            assertFalse(connection.isReadOnly());
            assertEquals(TsurugiJdbcTransactionType.OCC, connection.getTransactionType());
            assertEquals(TransactionType.SHORT, connection.getConfig().getLowTransactionOption().getType());

            connection.setReadOnly(true);
            assertTrue(connection.isReadOnly());
            assertEquals(TsurugiJdbcTransactionType.RTX, connection.getTransactionType());
            assertEquals(TransactionType.READ_ONLY, connection.getConfig().getLowTransactionOption().getType());
        }
    }

    @Test
    void setClientInfo() throws SQLException {
        try (var connection = createConnection()) {
            connection.setClientInfo(TsurugiConfig.TRANSACTION_TYPE, "LTX");
            connection.setClientInfo(TsurugiConfig.TRANSACTION_LABEL, "test");
            connection.setClientInfo(TsurugiConfig.INCLUDE_DDL, "true");
            connection.setClientInfo(TsurugiConfig.SCAN_PARALLEL, "123");

            assertEquals("LTX", connection.getClientInfo(TsurugiConfig.TRANSACTION_TYPE));
            assertEquals("test", connection.getClientInfo(TsurugiConfig.TRANSACTION_LABEL));
            assertEquals("true", connection.getClientInfo(TsurugiConfig.INCLUDE_DDL));
            assertEquals("123", connection.getClientInfo(TsurugiConfig.SCAN_PARALLEL));

            var info = connection.getClientInfo();
            assertEquals("LTX", info.getProperty(TsurugiConfig.TRANSACTION_TYPE));
            assertEquals("test", info.getProperty(TsurugiConfig.TRANSACTION_LABEL));
            assertEquals("true", info.getProperty(TsurugiConfig.INCLUDE_DDL));
            assertEquals("123", info.getProperty(TsurugiConfig.SCAN_PARALLEL));

            assertEquals(TsurugiJdbcTransactionType.LTX, connection.getTransactionType());
            assertEquals("test", connection.getTransactionLabel());
            assertTrue(connection.getTransactionIncludeDdl());
            assertEquals(OptionalInt.of(123), connection.getTransactionScanParallel());

            var lowTransactionOption = connection.getConfig().getLowTransactionOption();
            assertEquals(TransactionType.LONG, lowTransactionOption.getType());
            assertEquals("test", lowTransactionOption.getLabel());
            assertTrue(lowTransactionOption.getModifiesDefinitions());
            assertEquals(123, lowTransactionOption.getScanParallel());
        }
    }

    @Test
    void setClientInfo_error() throws SQLException {
        try (var connection = createConnection()) {
            {
                var e = assertThrows(SQLClientInfoException.class, () -> {
                    connection.setClientInfo("zzz", "abc");
                });
                assertEquals(Map.of("zzz", ClientInfoStatus.REASON_UNKNOWN_PROPERTY), e.getFailedProperties());
            }
            {
                var e = assertThrows(SQLClientInfoException.class, () -> {
                    connection.setClientInfo(TsurugiConfig.TRANSACTION_TYPE, "abc");
                });
                assertEquals(Map.of(TsurugiConfig.TRANSACTION_TYPE, ClientInfoStatus.REASON_VALUE_INVALID), e.getFailedProperties());
            }
            {
                var e = assertThrows(SQLClientInfoException.class, () -> {
                    connection.setClientInfo(TsurugiConfig.SCAN_PARALLEL, "abc");
                });
                assertEquals(Map.of(TsurugiConfig.SCAN_PARALLEL, ClientInfoStatus.REASON_VALUE_INVALID), e.getFailedProperties());
            }
        }
    }

    @Test
    void setClientInfo_properties() throws SQLException {
        try (var connection = createConnection()) {
            var properties = new Properties();
            properties.setProperty(TsurugiConfig.TRANSACTION_TYPE, "LTX");
            properties.setProperty(TsurugiConfig.TRANSACTION_LABEL, "test");
            properties.setProperty(TsurugiConfig.INCLUDE_DDL, "true");
            properties.setProperty(TsurugiConfig.SCAN_PARALLEL, "123");
            connection.setClientInfo(properties);

            assertEquals("LTX", connection.getClientInfo(TsurugiConfig.TRANSACTION_TYPE));
            assertEquals("test", connection.getClientInfo(TsurugiConfig.TRANSACTION_LABEL));
            assertEquals("true", connection.getClientInfo(TsurugiConfig.INCLUDE_DDL));
            assertEquals("123", connection.getClientInfo(TsurugiConfig.SCAN_PARALLEL));

            var info = connection.getClientInfo();
            assertEquals("LTX", info.getProperty(TsurugiConfig.TRANSACTION_TYPE));
            assertEquals("test", info.getProperty(TsurugiConfig.TRANSACTION_LABEL));
            assertEquals("true", info.getProperty(TsurugiConfig.INCLUDE_DDL));
            assertEquals("123", info.getProperty(TsurugiConfig.SCAN_PARALLEL));

            assertEquals(TsurugiJdbcTransactionType.LTX, connection.getTransactionType());
            assertEquals("test", connection.getTransactionLabel());
            assertTrue(connection.getTransactionIncludeDdl());
            assertEquals(OptionalInt.of(123), connection.getTransactionScanParallel());

            var lowTransactionOption = connection.getConfig().getLowTransactionOption();
            assertEquals(TransactionType.LONG, lowTransactionOption.getType());
            assertEquals("test", lowTransactionOption.getLabel());
            assertTrue(lowTransactionOption.getModifiesDefinitions());
            assertEquals(123, lowTransactionOption.getScanParallel());
        }
    }

    @Test
    void setClientInfo_properties_error() throws SQLException {
        try (var connection = createConnection()) {
            {
                var properties = new Properties();
                properties.setProperty(TsurugiConfig.TRANSACTION_TYPE, "LTX");
                properties.setProperty(TsurugiConfig.TRANSACTION_LABEL, "test");
                properties.setProperty(TsurugiConfig.INCLUDE_DDL, "true");
                properties.setProperty(TsurugiConfig.SCAN_PARALLEL, "abc"); // error

                var e = assertThrows(SQLClientInfoException.class, () -> {
                    connection.setClientInfo(properties);
                });
                assertEquals(Map.of(TsurugiConfig.SCAN_PARALLEL, ClientInfoStatus.REASON_VALUE_INVALID), e.getFailedProperties());
                assertEquals(0, e.getSuppressed().length);
            }
            {
                var properties = new Properties();
                properties.setProperty(TsurugiConfig.TRANSACTION_TYPE, "zzz"); // error
                properties.setProperty(TsurugiConfig.TRANSACTION_LABEL, "test");
                properties.setProperty(TsurugiConfig.INCLUDE_DDL, "true");
                properties.setProperty(TsurugiConfig.SCAN_PARALLEL, "abc"); // error

                var e = assertThrows(SQLClientInfoException.class, () -> {
                    connection.setClientInfo(properties);
                });
                assertEquals(Map.of(TsurugiConfig.TRANSACTION_TYPE, ClientInfoStatus.REASON_VALUE_INVALID, TsurugiConfig.SCAN_PARALLEL, ClientInfoStatus.REASON_VALUE_INVALID),
                        e.getFailedProperties());
                assertEquals(1, e.getSuppressed().length);
            }
        }
    }

    @Test
    void setClientInfo_properties_failure() throws SQLException {
        try (var connection = createConnection()) {
            var properties = new Properties();
            properties.setProperty(TsurugiConfig.TRANSACTION_TYPE, "LTX");
            properties.setProperty(TsurugiConfig.TRANSACTION_LABEL, "test");
            properties.setProperty(TsurugiConfig.INCLUDE_DDL, "true");
            properties.setProperty(TsurugiConfig.SCAN_PARALLEL, "123");
            properties.setProperty("zzz1", "abc");
            properties.setProperty("zzz2", "def");

            var e = assertThrows(SQLClientInfoException.class, () -> {
                connection.setClientInfo(properties);
            });
            assertEquals(Map.of("zzz1", ClientInfoStatus.REASON_UNKNOWN_PROPERTY, "zzz2", ClientInfoStatus.REASON_UNKNOWN_PROPERTY), e.getFailedProperties());
        }
    }

    @Test
    void close() throws SQLException {
        try (var connection = createConnection()) {
            assertFalse(connection.isClosed());
            assertTrue(connection.isValid(0));

            connection.close();
            assertTrue(connection.isClosed());
            assertFalse(connection.isValid(0));
        }
    }
}
