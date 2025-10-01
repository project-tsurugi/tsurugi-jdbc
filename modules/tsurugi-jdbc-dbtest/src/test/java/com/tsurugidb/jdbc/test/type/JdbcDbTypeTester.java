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
package com.tsurugidb.jdbc.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * Tsurugi JDBC type test.
 */
public abstract class JdbcDbTypeTester<T> extends JdbcDbTester {

    protected void createTable() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            statement.executeUpdate("drop table if exists test");
            statement.executeUpdate(String.format("create table test(" //
                    + " pk int primary key," //
                    + " value %s" //
                    + ")" //
                    , sqlType()));
        }
    }

    protected abstract String sqlType();

    protected String expectedSqlType() {
        return sqlType();
    }

    private void databaseMetaData() throws SQLException {
        try (var connection = createConnection()) {
            var metaData = connection.getMetaData();
            try (var rs = metaData.getColumns(null, null, "test", "%")) {
                assertTrue(rs.next());
                new ExpectedColumn(1, "pk").initialize("INT").notNull() //
                        .test(rs);

                assertTrue(rs.next());
                new ExpectedColumn(2, "value").initialize(expectedSqlType()) //
                        .test(rs);

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void iceaxeToJdbc() throws Exception {
        createTable();
        databaseMetaData();

        var values = values();
        insertIceaxe(values);
        var actual = selectJdbc();

        assertValueList(values, actual);
    }

    private void insertIceaxe(List<T> values) throws IOException, InterruptedException {
        var sql = "insert into test values(:pk, :value)";
        var mapping = TgParameterMapping.of(TgBindVariable.ofInt("pk"), bindVariable("value"));

        var connector = TsurugiConnector.of(JdbcDbTestConnector.getEndPoint(), JdbcDbTestConnector.getIceaxeCredential());
        try (var session = connector.createSession(); //
                var ps = session.createStatement(sql, mapping)) {
            var manager = session.createTransactionManager(TgTxOption.ofOCC());

            manager.execute(transaction -> {
                int i = 0;
                for (var value : values) {
                    var parameter = TgBindParameters.of(TgBindParameter.of("pk", i), bindParameter("value", value));
                    transaction.executeAndGetCountDetail(ps, parameter);

                    i++;
                }
            });
        }
    }

    private List<T> selectJdbc() throws SQLException {
        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("select * from test order by pk")) {
                resultSetMetaData(rs);

                var actual = new ArrayList<T>();
                int rowIndex = 0;
                while (rs.next()) {
                    int pk = rs.getInt(1);
                    assertEquals(rowIndex, pk, "pk");

                    T value = get(rs, 2);
                    actual.add(value);

                    rowIndex++;
                }

                return actual;
            }
        }
    }

    private void resultSetMetaData(TsurugiJdbcResultSet rs) throws SQLException {
        var metaData = rs.getMetaData();

        int columnCount = metaData.getColumnCount();
        assertEquals(2, columnCount);

        new ExpectedColumn(1, "pk").initialize("INT").notNull() //
                .test(metaData, 1);
        new ExpectedColumn(2, "value").initialize(expectedSqlType()) //
                .test(metaData, 2);
    }

    @Test
    @SuppressWarnings("deprecation")
    void resultSetPattern() throws Exception {
        createTable();

        var values = values();
        insertJdbc(values);

        try (var connection = createConnection(); //
                var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("select * from test order by pk")) {
                int rowIndex = 0;
                while (rs.next()) {
                    int pk = rs.getInt(1);
                    assertEquals(rowIndex, pk, "pk");

                    T expected = values.get(rowIndex);
                    patternTest(expected, rs, ResultSet::getObject, Object.class);
                    patternTest(expected, rs, ResultSet::getString, String.class);
                    patternTest(expected, rs, ResultSet::getBoolean, Boolean.class);
                    patternTest(expected, rs, ResultSet::getByte, Byte.class);
                    patternTest(expected, rs, ResultSet::getShort, Short.class);
                    patternTest(expected, rs, ResultSet::getInt, Integer.class);
                    patternTest(expected, rs, ResultSet::getLong, Long.class);
                    patternTest(expected, rs, ResultSet::getFloat, Float.class);
                    patternTest(expected, rs, ResultSet::getDouble, Double.class);
                    patternTest(expected, rs, ResultSet::getBigDecimal, BigDecimal.class);
                    patternTest(expected, rs, ResultSet::getBytes, byte[].class);
                    patternTest(expected, rs, ResultSet::getDate, java.sql.Date.class);
                    patternTest(expected, rs, ResultSet::getTime, java.sql.Time.class);
                    patternTest(expected, rs, ResultSet::getTimestamp, java.sql.Timestamp.class);
                    patternTest(expected, rs, (s, i) -> s.getObject(i, LocalDate.class), LocalDate.class);
                    patternTest(expected, rs, (s, i) -> s.getObject(i, LocalTime.class), LocalTime.class);
                    patternTest(expected, rs, (s, i) -> s.getObject(i, LocalDateTime.class), LocalDateTime.class);
                    patternTest(expected, rs, (s, i) -> s.getObject(i, OffsetTime.class), OffsetTime.class);
                    patternTest(expected, rs, (s, i) -> s.getObject(i, OffsetDateTime.class), OffsetDateTime.class);
                    patternTest(expected, rs, ResultSet::getAsciiStream, InputStream.class);
                    patternTest(expected, rs, ResultSet::getUnicodeStream, InputStream.class);
                    patternTest(expected, rs, ResultSet::getBinaryStream, InputStream.class);
                    patternTest(expected, rs, ResultSet::getCharacterStream, Reader.class);

                    rowIndex++;
                }
            }
        }
    }

    @FunctionalInterface
    private interface Getter {
        Object get(ResultSet rs, int columnIndex) throws SQLException;
    }

    private static final Set<Class<?>> PRIMITIVE_SET = Set.of(Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class);

    private void patternTest(T expected, ResultSet rs, Getter getter, Class<?> valueType) throws SQLException {
        Object value;
        try {
            value = getter.get(rs, 2);
        } catch (SQLDataException e) {
            try {
                assertException(expected, valueType, e);
                return;
            } catch (Throwable t) {
                LOG.error("selectPattern() FAIL. expected={}, valueType={}, e.message={}", expected, valueType.getCanonicalName(), e.getMessage(), t);
                throw t;
            }
        }
        assertEquals(expected == null, rs.wasNull());

        try {
            if (expected == null) {
                if (PRIMITIVE_SET.contains(valueType)) {
                    assertNotNull(value);
                    assertEquals(0, ((Number) value).doubleValue());
                } else if (value instanceof Boolean) {
                    assertNotNull(value);
                    assertFalse((Boolean) value);
                } else {
                    assertNull(value);
                }
            } else {
                assertNotNull(value);
                assertValue(expected, valueType, value);
            }
        } catch (Throwable e) {
            LOG.error("selectPattern() FAIL. expected={}, valueType={}, actual={}", expected, valueType.getCanonicalName(), value, e);
            throw e;
        }
    }

    protected abstract void assertException(T expected, Class<?> valueType, SQLDataException e);

    protected abstract void assertValue(@Nonnull T expected, Class<?> valueType, @Nonnull Object actual);

    @Test
    void jdbcToIceaxe() throws Exception {
        createTable();

        var values = values();
        insertJdbc(values);
        var actual = selectIceaxe();

        assertValueList(values, actual);
    }

    protected void insertJdbc(List<T> values) throws SQLException {
        try (var connection = createConnection(); //
                var ps = connection.prepareStatement("insert into test values(?, ?)")) {
            connection.setAutoCommit(false);

            int pk = 0;
            for (T value : values) {
                ps.setInt(1, pk);
                setParameter(ps, 2, value);

                if (pk == 0) {
                    parameterMetaData(ps);
                }

                assertEquals(1, ps.executeUpdate());

                pk++;
            }

            connection.commit();
        }
    }

    private void parameterMetaData(TsurugiJdbcPreparedStatement ps) throws SQLException {
        var metaData = ps.getParameterMetaData();

        int parameterCount = metaData.getParameterCount();
        assertEquals(2, parameterCount);

        new ExpectedColumn(1, "pk").initialize("INT").notNull() //
                .test(metaData, 1);
        new ExpectedColumn(2, "value").initialize(expectedSqlType()) //
                .test(metaData, 2);
    }

    private List<T> selectIceaxe() throws IOException, InterruptedException {
        var actual = new ArrayList<T>();

        var connector = TsurugiConnector.of(JdbcDbTestConnector.getEndPoint(), JdbcDbTestConnector.getIceaxeCredential());
        try (var session = connector.createSession()) {
            var manager = session.createTransactionManager(TgTxOption.ofOCC());

            var list = manager.executeAndGetList("select * from test order by pk");
            int i = 0;
            for (var entity : list) {
                int pk = entity.getInt("pk");
                assertEquals(i, pk);

                T value;
                if (entity.getValueOrNull("value") == null) {
                    value = null;
                } else {
                    value = get(entity, "value");
                }
                actual.add(value);

                i++;
            }
        }

        return actual;
    }

    protected abstract List<T> values();

    protected abstract TgBindVariable<T> bindVariable(String name);

    protected abstract TgBindParameter bindParameter(String name, T value);

    protected abstract T get(TsurugiResultEntity entity, String name);

    protected abstract T get(ResultSet rs, int columnIndex) throws SQLException;

    protected abstract void setParameter(PreparedStatement ps, int parameterIndex, T value) throws SQLException;

    protected void assertValueList(List<T> expected, List<T> actual) {
        try {
            assertIterableEquals(expected, actual);
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), expected, actual);
            throw e;
        }
    }
}
