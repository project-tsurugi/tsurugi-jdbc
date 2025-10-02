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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIME test.
 */
public class JdbcDbTypeLocalTimeTest extends JdbcDbTypeTester<LocalTime> {

    @Override
    protected String sqlType() {
        return "time";
    }

    @Override
    protected List<LocalTime> values() {
        var list = new ArrayList<LocalTime>();
        list.add(LocalTime.now());
        list.add(LocalTime.of(0, 0, 0));
        list.add(LocalTime.of(1, 2, 3, 456));
        list.add(LocalTime.of(1, 2, 3, 456_000_000));
        list.add(LocalTime.of(12, 30, 59, 123456789));
        list.add(LocalTime.of(23, 59, 59, 999_999_999));
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<LocalTime> bindVariable(String name) {
        return TgBindVariable.ofTime(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, LocalTime value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected LocalTime get(TsurugiResultEntity entity, String name) {
        return entity.getTime(name);
    }

    @Override
    protected LocalTime get(ResultSet rs, int columnIndex) throws SQLException {
        LocalTime value = (LocalTime) rs.getObject(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, LocalTime value) throws SQLException {
        if (value != null) {
            ps.setObject(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIME);
        }
    }

    @Override
    protected void assertException(LocalTime expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case OBJECT:
        case STRING:
        case TIME:
        case LOCAL_TIME:
        case OFFSET_TIME:
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(LocalTime expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(expected.toString(), actual);
            return;
        case TIME:
            assertEquals(toSqlTime(expected), actual);
            return;
        case OFFSET_TIME:
            assertEquals(toOffsetTime(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Time toSqlTime(LocalTime value) {
        return java.sql.Time.valueOf(value);
    }

    private OffsetTime toOffsetTime(LocalTime value) {
        return value.atOffset(ZoneOffset.UTC);
    }
}
