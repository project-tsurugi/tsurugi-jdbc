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
 * Tsurugi JDBC FLOAT8 test.
 */
public class JdbcDbTypeFloat8Test extends JdbcDbTypeTester<Double> {

    @Override
    protected String sqlType() {
        return "double";
    }

    @Override
    protected List<Double> values() {
        var list = new ArrayList<Double>();
        list.add(Double.MIN_VALUE);
        list.add(-1d);
        list.add(0d);
        list.add(1d);
        list.add(123.4d);
        list.add(Double.MAX_VALUE);
        list.add(null);
        list.add(Double.NEGATIVE_INFINITY);
        list.add(Double.POSITIVE_INFINITY);
        list.add(Double.NaN);
        return list;
    }

    @Override
    protected TgBindVariable<Double> bindVariable(String name) {
        return TgBindVariable.ofDouble(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, Double value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected Double get(TsurugiResultEntity entity, String name) {
        return entity.getDouble(name);
    }

    @Override
    protected Double get(ResultSet rs, int columnIndex) throws SQLException {
        double value = rs.getDouble(columnIndex);
        if (rs.wasNull()) {
            return null;
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Double value) throws SQLException {
        if (value != null) {
            ps.setDouble(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.DOUBLE);
        }
    }

    private static final Set<Class<?>> SUPPORT_SET = Set.of(String.class, Boolean.class, Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, BigDecimal.class);

    @Override
    protected void assertException(Double expected, Class<?> valueType, SQLDataException e) {
        if (valueType == Boolean.class) {
            double v = expected.doubleValue();
            if (v == 0 || v == 1) {
                fail(e);
            } else {
                assertTrue(e.getMessage().contains("Cannot cast to boolean"));
            }
            return;
        }
        if (valueType == BigDecimal.class) {
            if (expected.isInfinite() || expected.isNaN()) {
                assertTrue(e.getMessage().contains("convertToDecimal error"));
            } else {
                fail(e);
            }
            return;
        }

        if (SUPPORT_SET.contains(valueType)) {
            fail(e);
        }

        assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
    }

    @Override
    protected void assertValue(Double expected, Class<?> valueType, Object actual) {
        if (valueType == String.class) {
            assertEquals(Double.toString(expected), actual);
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
