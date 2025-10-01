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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC INT4 test.
 */
public class JdbcDbTypeInt4Test extends JdbcDbTypeTester<Integer> {

    @Override
    protected String sqlType() {
        return "int";
    }

    @Override
    protected List<Integer> values() {
        var list = new ArrayList<Integer>();
        list.add(Integer.MIN_VALUE);
        list.add(-1);
        list.add(0);
        list.add(1);
        list.add(123);
        list.add(Integer.MAX_VALUE);
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<Integer> bindVariable(String name) {
        return TgBindVariable.ofInt(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, Integer value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected Integer get(TsurugiResultEntity entity, String name) {
        return entity.getInt(name);
    }

    @Override
    protected Integer get(ResultSet rs, int columnIndex) throws SQLException {
        int value = rs.getInt(columnIndex);
        if (rs.wasNull()) {
            assertEquals(0, value);
            return null;
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Integer value) throws SQLException {
        if (value != null) {
            ps.setInt(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.INTEGER);
        }
    }

    private static final Set<Class<?>> SUPPORT_SET = Set.of(String.class, Boolean.class, Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, BigDecimal.class);

    @Override
    protected void assertException(Integer expected, Class<?> valueType, SQLDataException e) {
        if (valueType == Boolean.class) {
            double v = expected.doubleValue();
            if (v == 0 || v == 1) {
                fail(e);
            } else {
                assertTrue(e.getMessage().contains("Cannot cast to boolean"));
            }
            return;
        }

        if (SUPPORT_SET.contains(valueType)) {
            fail(e);
        }

        assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
    }

    @Override
    protected void assertValue(Integer expected, Class<?> valueType, Object actual) {
        if (valueType == String.class) {
            assertEquals(Integer.toString(expected), actual);
            return;
        }
        if (valueType == Boolean.class) {
            assertEquals(expected != 0, actual);
            return;
        }
        if (valueType == Byte.class) {
            assertEquals(expected.byteValue(), actual);
            return;
        }
        if (valueType == Short.class) {
            assertEquals(expected.shortValue(), actual);
            return;
        }
        if (valueType == Integer.class) {
            assertEquals(expected.intValue(), actual);
            return;
        }
        if (valueType == Long.class) {
            assertEquals(expected.longValue(), actual);
            return;
        }
        if (valueType == Float.class) {
            assertEquals(expected.floatValue(), actual);
            return;
        }
        if (valueType == Double.class) {
            assertEquals(expected.doubleValue(), actual);
            return;
        }
        if (valueType == BigDecimal.class) {
            assertEquals(BigDecimal.valueOf(expected), actual);
            return;
        }

        assertEquals(expected, actual, "valueType=" + valueType.getCanonicalName());
    }
}
