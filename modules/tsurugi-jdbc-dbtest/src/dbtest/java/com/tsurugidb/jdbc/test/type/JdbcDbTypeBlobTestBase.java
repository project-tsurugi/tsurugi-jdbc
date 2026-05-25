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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.Blob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.jdbc.statement.type.TsurugiJdbcBlob;

/**
 * Tsurugi JDBC BLOB test.
 */
public abstract class JdbcDbTypeBlobTestBase extends JdbcDbTypeTester<Blob> {

    @Override
    protected String sqlType() {
        return "blob";
    }

    @Override
    protected List<Blob> values() {
        var list = new ArrayList<Blob>();
        list.add(new TsurugiJdbcBlob(new byte[0]));
        list.add(new TsurugiJdbcBlob(new byte[] { 1, 2, 3, 100, (byte) 0xff }));
        list.add(null);
        return list;
    }

    @Override
    protected void modifyParameterMetaDataExcepted(ExpectedColumn expected) {
        expected.dataType(JDBCType.BLOB).typeBaseName("BLOB");
    }

    @Override
    protected TgBindVariable<Blob> bindVariable(String name) {
        class TgBindVariableTestBlob extends TgBindVariable<Blob> {
            protected TgBindVariableTestBlob(@Nonnull String name) {
                super(name, TgDataType.BLOB);
            }

            @Override
            public TgBindParameter bind(@Nullable Blob value) {
                byte[] bytes = toBytes(value);
                try {
                    return TgBindParameter.ofBlob(name(), bytes);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            }

            @Override
            public TgBindVariableTestBlob clone(@Nonnull String name) {
                return new TgBindVariableTestBlob(name);
            }
        }

        return new TgBindVariableTestBlob(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, Blob value) {
        byte[] bytes = toBytes(value);
        try {
            return TgBindParameter.ofBlob(name, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected Blob get(TsurugiResultEntity entity, String name) {
        TgBlob value = entity.getBlob(name);
        try {
            return new TsurugiJdbcBlob(value.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected Blob get(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getBlob(columnIndex);
    }

    @Override
    protected Blob getPersistence(ResultSet rs, int columnIndex) throws SQLException {
        Blob blob = rs.getBlob(columnIndex);
        if (blob != null) {
            blob.length(); // create cache
        }
        return blob;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Blob value) throws SQLException {
        if (value != null) {
            ps.setBlob(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.BLOB);
        }
    }

    @Override
    protected void assertException(Blob expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(Blob expected, ValueType valueType, Object actual) {
        byte[] expectedBytes = toBytes(expected);
        switch (valueType) {
        case BYTES:
            assertArrayEquals(expectedBytes, (byte[]) actual);
            return;
        case BINARY_STREAM:
            try {
                var bytes = ((InputStream) actual).readAllBytes();
                assertArrayEquals(expectedBytes, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            return;
        case BLOB:
        case OBJECT:
            Blob actualBlob = (Blob) actual;
            byte[] actualBytes = toBytes(actualBlob);
            assertArrayEquals(expectedBytes, actualBytes);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    @Override
    protected void assertValueList(List<Blob> expected, List<Blob> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                byte[] expectedBytes = toBytes(expected.get(i));
                byte[] actualBytes = toBytes(actual.get(i));
                assertArrayEquals(expectedBytes, actualBytes);
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), toString(expected), toString(actual));
            throw e;
        }
    }

    private static byte[] toBytes(Blob blob) {
        if (blob == null) {
            return null;
        }

        try (var is = blob.getBinaryStream()) {
            return is.readAllBytes();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private static String toString(List<Blob> list) {
        return list.stream().map(JdbcDbTypeBlobTestBase::toBytes).map(Arrays::toString).collect(Collectors.joining(", ", "[", "]"));
    }
}
