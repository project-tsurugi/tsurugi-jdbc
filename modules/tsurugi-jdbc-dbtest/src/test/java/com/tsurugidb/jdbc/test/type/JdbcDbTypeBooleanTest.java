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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC BOOLEAN test.
 */
public class JdbcDbTypeBooleanTest extends JdbcDbTypeTester<Boolean> {

    @Override
    protected String sqlType() {
        return "int"; // TODO boolean
    }

    @Override
    protected List<Boolean> values() {
        var list = new ArrayList<Boolean>();
        list.add(false);
        list.add(true);
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<Boolean> bindVariable(String name) {
        return new TgBindVariable<Boolean>(name, TgDataType.INT) { // TODO BOOLEAN
            @Override
            public TgBindParameter bind(Boolean value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TgBindVariable<Boolean> clone(String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected TgBindParameter bindParameter(String name, Boolean value) {
        Integer n;
        if (value == null) {
            n = null;
        } else if (value) {
            n = 1;
        } else {
            n = 0;
        }
        return TgBindParameter.of(name, n); // TODO boolean
    }

    @Override
    protected Boolean get(TsurugiResultEntity entity, String name) {
        return entity.getBoolean(name);
    }

    @Override
    protected Boolean get(ResultSet rs, int columnIndex) throws SQLException {
        boolean value = rs.getBoolean(columnIndex);
        if (rs.wasNull()) {
            assertFalse(value);
            return null;
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, Boolean value) throws SQLException {
        if (value != null) {
            ps.setInt(parameterIndex, value ? 1 : 0); // TODO setBoolean
        } else {
            ps.setNull(parameterIndex, java.sql.Types.INTEGER); // TODO BOOLEAN
        }
    }

    @Override
    protected void assertException(Boolean expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case STRING:
        case BOOLEAN:
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
    protected void assertValue(Boolean expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(Integer.toString(expectedInt(expected)), actual);
            return;
        case BOOLEAN:
            assertEquals(expected, actual);
            return;
        case BYTE:
            assertEquals((byte) expectedInt(expected), actual);
            return;
        case SHORT:
            assertEquals((short) expectedInt(expected), actual);
            return;
        case OBJECT:
        case INT:
            assertEquals(expectedInt(expected), actual);
            return;
        case LONG:
            assertEquals((long) expectedInt(expected), actual);
            return;
        case FLOAT:
            assertEquals((float) expectedInt(expected), actual);
            return;
        case DOUBLE:
            assertEquals((double) expectedInt(expected), actual);
            return;
        case DECIMAL:
            assertEquals(BigDecimal.valueOf(expectedInt(expected)), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private static int expectedInt(boolean value) {
        return value ? 1 : 0;
    }
}
