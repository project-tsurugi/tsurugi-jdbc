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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC BINARY test.
 */
public class JdbcDbTypeBinaryTest extends JdbcDbTypeTester<byte[]> {

    private static final int LENGTH = 8;

    @Override
    protected String sqlType() {
        return "binary(" + LENGTH + ")";
    }

    @Override
    protected List<byte[]> values() {
        var list = new ArrayList<byte[]>();
        list.add(new byte[0]);
        list.add(new byte[] { 1, 2, 3, 100, (byte) 0xff });
        list.add(null);
        return list;
    }

    @Override
    protected void modifyParameterMetaDataExcepted(ExpectedColumn expected) {
        expected.dataType(JDBCType.VARBINARY).typeBaseName("VARBINARY");
    }

    @Override
    protected TgBindVariable<byte[]> bindVariable(String name) {
        return TgBindVariable.ofBytes(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, byte[] value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected byte[] get(TsurugiResultEntity entity, String name) {
        return entity.getBytes(name);
    }

    @Override
    protected byte[] get(ResultSet rs, int columnIndex) throws SQLException {
        byte[] value = rs.getBytes(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, byte[] value) throws SQLException {
        if (value != null) {
            ps.setBytes(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.BINARY);
        }
    }

    @Override
    protected void assertException(byte[] expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(byte[] expected, ValueType valueType, Object actual) {
        expected = expectedBinary(expected);

        switch (valueType) {
        case BYTES:
        case OBJECT:
            assertArrayEquals(expected, (byte[]) actual);
            return;
        case BINARY_STREAM:
            try {
                var bytes = ((InputStream) actual).readAllBytes();
                assertArrayEquals(expected, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    @Override
    protected void assertValueList(List<byte[]> expected, List<byte[]> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                byte[] e = expectedBinary(expected.get(i));
                assertArrayEquals(e, actual.get(i));
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), toString(expected), toString(actual));
            throw e;
        }
    }

    private static byte[] expectedBinary(byte[] value) {
        if (value == null) {
            return null;
        }
        if (value.length >= LENGTH) {
            return value;
        }

        return Arrays.copyOf(value, LENGTH);
    }

    private static String toString(List<byte[]> list) {
        return list.stream().map(Arrays::toString).collect(Collectors.joining(", ", "[", "]"));
    }
}
