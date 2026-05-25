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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.jdbc.statement.type.TsurugiJdbcClob;

/**
 * Tsurugi JDBC CLOB test.
 */
public class JdbcDbTypeClobTest extends JdbcDbTypeTester<Clob> {

    @Override
    protected String sqlType() {
        return "clob";
    }

    @Override
    protected List<Clob> values() {
        var list = new ArrayList<Clob>();
        list.add(new TsurugiJdbcClob(new StringBuilder()));
        list.add(new TsurugiJdbcClob(new StringBuilder("abc")));
        list.add(null);
        return list;
    }

    @Override
    protected void modifyParameterMetaDataExcepted(ExpectedColumn expected) {
        expected.dataType(JDBCType.CLOB).typeBaseName("CLOB");
    }

    @Override
    protected TgBindVariable<Clob> bindVariable(String name) {
        class TgBindVariableTestClob extends TgBindVariable<Clob> {
            protected TgBindVariableTestClob(@Nonnull String name) {
                super(name, TgDataType.CLOB);
            }

            @Override
            public TgBindParameter bind(@Nullable Clob value) {
                String data = JdbcDbTypeClobTest.toString(value);
                try {
                    return TgBindParameter.ofClob(name(), data);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            }

            @Override
            public TgBindVariableTestClob clone(@Nonnull String name) {
                return new TgBindVariableTestClob(name);
            }
        }

        return new TgBindVariableTestClob(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, Clob value) {
        String data = toString(value);
        try {
            return TgBindParameter.ofClob(name, data);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected Clob get(TsurugiResultEntity entity, String name) {
        TgClob value = entity.getClob(name);
        try {
            return new TsurugiJdbcClob(new StringBuilder(value.readString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected Clob get(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getClob(columnIndex);
    }

    @Override
    protected Clob getPersistence(ResultSet rs, int columnIndex) throws SQLException {
        Clob clob = rs.getClob(columnIndex);
        if (clob != null) {
            clob.length(); // create cache
        }
        return clob;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Clob value) throws SQLException {
        if (value != null) {
            ps.setClob(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.CLOB);
        }
    }

    @Override
    protected void assertException(Clob expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(Clob expected, ValueType valueType, Object actual) {
        String expectedStr = toString(expected);
        switch (valueType) {
        case STRING:
            assertEquals(expectedStr, (String) actual);
            return;
        case CHARACTER_STREAM:
            assertEquals(expectedStr, readAllString((Reader) actual));
            return;
        case CLOB:
        case OBJECT:
            Clob actualClob = (Clob) actual;
            String actualStr = toString(actualClob);
            assertEquals(expectedStr, actualStr);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private static String readAllString(Reader reader) {
        try (reader; var writer = new StringWriter()) {
            reader.transferTo(writer);
            return writer.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected void assertValueList(List<Clob> expected, List<Clob> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                String expectedStr = toString(expected.get(i));
                String actualStr = toString(actual.get(i));
                assertEquals(expectedStr, actualStr);
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), toString(expected), toString(actual));
            throw e;
        }
    }

    private static String toString(Clob clob) {
        if (clob == null) {
            return null;
        }

        try {
            return readAllString(clob.getCharacterStream());
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String toString(List<Clob> list) {
        return list.stream().map(JdbcDbTypeClobTest::toString).collect(Collectors.joining(", ", "[", "]"));
    }
}
