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
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * Tsurugi JDBC DECIMAL test.
 */
public class JdbcDbTypeDecimalTest extends JdbcDbTypeTester<BigDecimal> {

    private static final int SCALE = 1;

    @Override
    protected String sqlType() {
        return "decimal(38, " + SCALE + ")";
    }

    private static final BigDecimal MIN_DECIMAL = new BigDecimal("-" + "9".repeat(37) + ".9");
    private static final BigDecimal MAX_DECIMAL = new BigDecimal("9".repeat(37) + ".9");

    @Override
    protected List<BigDecimal> values() {
        var list = new ArrayList<BigDecimal>();
        list.add(MIN_DECIMAL);
        list.add(BigDecimal.valueOf(-1));
        list.add(BigDecimal.valueOf(0));
        list.add(BigDecimal.valueOf(1));
        list.add(new BigDecimal("123.4"));
        list.add(new BigDecimal("-123.4"));
        list.add(MAX_DECIMAL);
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<BigDecimal> bindVariable(String name) {
        return TgBindVariable.ofDecimal(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, BigDecimal value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected BigDecimal get(TsurugiResultEntity entity, String name) {
        return entity.getDecimal(name);
    }

    @Override
    protected BigDecimal get(ResultSet rs, int columnIndex) throws SQLException {
        BigDecimal value = rs.getBigDecimal(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, BigDecimal value) throws SQLException {
        if (value != null) {
            ps.setBigDecimal(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.DECIMAL);
        }
    }

    @Override
    protected void assertValueList(List<BigDecimal> expected, List<BigDecimal> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            var e = expected.get(i);
            var a = actual.get(i);

            if (e == null) {
                assertNull(a);
                continue;
            }

            assertEquals(e.setScale(SCALE), a);
        }
    }

    @Override
    protected void assertException(BigDecimal expected, ValueType valueType, SQLDataException e) {
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
        case DECIMAL:
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(BigDecimal expected, ValueType valueType, Object actual) {
        expected = expected.setScale(SCALE);

        switch (valueType) {
        case STRING:
            assertEquals(expected.toPlainString(), actual);
            return;
        case BOOLEAN:
            assertEquals(expected.doubleValue() != 0, actual);
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
            assertEquals(expected, actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }
}
