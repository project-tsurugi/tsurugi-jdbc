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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC DATE test.
 */
public class JdbcDbTypeSqlDateTest extends JdbcDbTypeTester<java.sql.Date> {

    @Override
    protected String sqlType() {
        return "date";
    }

    @Override
    protected List<java.sql.Date> values() {
        var list = new ArrayList<java.sql.Date>();
        list.add(date(LocalDate.now()));
        list.add(date(1970, 1, 1));
        list.add(date(-1, 1, 1));
        list.add(date(0, 1, 1));
        list.add(date(1, 1, 1));
        list.add(date(9999, 12, 31));
        list.add(null);
        return list;
    }

    private java.sql.Date date(int year, int month, int day) {
        return date(LocalDate.of(year, month, day));
    }

    private java.sql.Date date(LocalDate value) {
        return toSqlDate(value);
    }

    @Override
    protected TgBindVariable<java.sql.Date> bindVariable(String name) {
        return new TgBindVariable<>(name, TgDataType.DATE) {
            @Override
            public TgBindParameter bind(java.sql.Date value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TgBindVariable<java.sql.Date> clone(String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected TgBindParameter bindParameter(String name, java.sql.Date value) {
        var date = toLocalDate(value);
        return TgBindParameter.of(name, date);
    }

    @Override
    protected java.sql.Date get(TsurugiResultEntity entity, String name) {
        LocalDate value = entity.getDate(name);
        if (value == null) {
            return null;
        }
        return toSqlDate(value);
    }

    @Override
    protected java.sql.Date get(ResultSet rs, int columnIndex) throws SQLException {
        java.sql.Date value = rs.getDate(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, java.sql.Date value) throws SQLException {
        if (value != null) {
            ps.setDate(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.DATE);
        }
    }

    @Override
    protected void assertException(java.sql.Date expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case OBJECT:
        case STRING:
        case DATE:
        case TIMESTAMP:
        case LOCAL_DATE:
        case LOCAL_DATE_TIME:
        case OFFSET_DATE_TIME:
        case ZONED_DATE_TIME:
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(java.sql.Date expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(toLocalDate(expected).toString(), actual);
            return;
        case DATE:
            assertEquals(expected, actual);
            return;
        case TIMESTAMP:
            assertEquals(toSqlTimestamp(expected), actual);
            return;
        case LOCAL_DATE:
        case OBJECT:
            assertEquals(toLocalDate(expected), actual);
            return;
        case LOCAL_DATE_TIME:
            assertEquals(toLocalDate(expected).atStartOfDay(), actual);
            return;
        case OFFSET_DATE_TIME:
            assertEquals(toZonedDateTime(expected).toOffsetDateTime(), actual);
            return;
        case ZONED_DATE_TIME:
            assertEquals(toZonedDateTime(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Date toSqlDate(LocalDate value) {
        long epochDay = value.toEpochDay();
        return new java.sql.Date(TimeUnit.DAYS.toMillis(epochDay));
    }

    private java.sql.Timestamp toSqlTimestamp(java.sql.Date value) {
        var zone = ZoneId.systemDefault();
        var zdt = toLocalDate(value).atStartOfDay(zone);
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    private LocalDate toLocalDate(java.sql.Date value) {
        if (value == null) {
            return null;
        }
        long epochMilli = value.getTime();
        return LocalDate.ofEpochDay(TimeUnit.MILLISECONDS.toDays(epochMilli));
    }

    private ZonedDateTime toZonedDateTime(java.sql.Date value) {
        var ldt = toLocalDate(value);
        return ldt.atStartOfDay(ZoneId.systemDefault());
    }
}
