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

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC FLOAT4 test.
 */
public class JdbcDbTypeFloat4Test extends JdbcDbTypeTester<Float> {

    @Override
    protected String sqlType() {
        return "real";
    }

    @Override
    protected List<Float> values() {
        var list = new ArrayList<Float>();
        list.add(Float.MIN_VALUE);
        list.add(-1f);
        list.add(0f);
        list.add(1f);
        list.add(123.4f);
        list.add(Float.MAX_VALUE);
        list.add(null);
        list.add(Float.NEGATIVE_INFINITY);
        list.add(Float.POSITIVE_INFINITY);
        list.add(Float.NaN);
        return list;
    }

    @Override
    protected TgBindVariable<Float> bindVariable(String name) {
        return TgBindVariable.ofFloat(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, Float value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected Float get(TsurugiResultEntity entity, String name) {
        return entity.getFloat(name);
    }

    @Override
    protected Float get(ResultSet rs, int columnIndex) throws SQLException {
        float value = rs.getFloat(columnIndex);
        if (rs.wasNull()) {
            assertEquals(0, value);
            return null;
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Float value) throws SQLException {
        if (value != null) {
            ps.setFloat(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.REAL);
        }
    }

    @Override
    protected void assertException(Float expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case BOOLEAN:
            double v = expected.doubleValue();
            if (v == 0 || v == 1) {
                fail(e);
            } else {
                assertTrue(e.getMessage().contains("Cannot cast to boolean"));
            }
            return;
        case STRING:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
            fail(e);
            return;
        case DECIMAL:
            if (expected.isInfinite() || expected.isNaN()) {
                assertTrue(e.getMessage().contains("convertToDecimal error"));
            } else {
                fail(e);
            }
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(Float expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(Float.toString(expected), actual);
            return;
        case BOOLEAN:
            assertEquals(expected != 0, actual);
            return;
        case BYTE:
            assertEquals(expected.byteValue(), actual);
            return;
        case SHORT:
            assertEquals(expected.shortValue(), actual);
            return;
        case INT:
            assertEquals(expected.intValue(), actual);
            return;
        case LONG:
            assertEquals(expected.longValue(), actual);
            return;
        case FLOAT:
            assertEquals(expected.floatValue(), actual);
            return;
        case DOUBLE:
            assertEquals(expected.doubleValue(), actual);
            return;
        case DECIMAL:
            assertEquals(BigDecimal.valueOf(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }
}
