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
import java.sql.Time;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIME test.
 */
public class JdbcDbTypeSqlTimeTest extends JdbcDbTypeTester<java.sql.Time> {

    @Override
    protected String sqlType() {
        return "time";
    }

    @Override
    protected List<java.sql.Time> values() {
        var list = new ArrayList<java.sql.Time>();
        list.add(time(LocalTime.now()));
        list.add(time(0, 0, 0));
        list.add(time(1, 2, 3));
        list.add(time(12, 30, 59));
        list.add(time(23, 59, 59));
        list.add(null);
        return list;
    }

    private java.sql.Time time(int hour, int minute, int second) {
        return time(LocalTime.of(hour, minute, second));
    }

    private java.sql.Time time(LocalTime value) {
        return java.sql.Time.valueOf(value);
    }

    @Override
    protected TgBindVariable<java.sql.Time> bindVariable(String name) {
        return new TgBindVariable<>(name, TgDataType.TIME) {
            @Override
            public TgBindParameter bind(Time value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TgBindVariable<Time> clone(String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected TgBindParameter bindParameter(String name, java.sql.Time value) {
        var time = toLocalTime(value);
        return TgBindParameter.of(name, time);
    }

    @Override
    protected java.sql.Time get(TsurugiResultEntity entity, String name) {
        LocalTime value = entity.getTime(name);
        return java.sql.Time.valueOf(value);
    }

    @Override
    protected java.sql.Time get(ResultSet rs, int columnIndex) throws SQLException {
        var value = rs.getTime(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, java.sql.Time value) throws SQLException {
        if (value != null) {
            ps.setTime(parameterIndex, value);
            ;
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIME);
        }
    }

    @Override
    protected void assertException(java.sql.Time expected, ValueType valueType, SQLDataException e) {
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
    protected void assertValue(java.sql.Time expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(toLocalTime(expected).toString(), actual);
            return;
        case LOCAL_TIME:
        case OBJECT:
            assertEquals(toLocalTime(expected), actual);
            return;
        case OFFSET_TIME:
            assertEquals(toOffsetTime(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private LocalTime toLocalTime(java.sql.Time value) {
        if (value == null) {
            return null;
        }
        return value.toLocalTime();
    }

    private OffsetTime toOffsetTime(java.sql.Time value) {
        return toLocalTime(value).atOffset(ZoneOffset.UTC);
    }
}
